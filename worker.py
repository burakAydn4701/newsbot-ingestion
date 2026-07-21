# -*- coding: utf-8 -*-
import os
# Load the embedding model from the local HF cache without contacting
# huggingface.co, so a slow/unreachable HF can't hang worker startup.
# (Must be set before importing ingestion.embeddings, which loads the model.)
os.environ.setdefault("HF_HUB_OFFLINE", "1")

import time
from bs4 import BeautifulSoup
from ingestion.sql_server import fetch_candidate_ids, fetch_articles_by_ids
from ingestion.postgres import (
    connect_postgres,
    insert_article,
    insert_chunks,
    fetch_existing_ids,
)
from ingestion.chunks import chunk_text
from ingestion.embeddings import get_embeddings
from config import SLEEP_SECONDS

# How many recent days to re-scan each cycle for un-ingested articles. Must be
# comfortably larger than the longest delay between an article appearing and
# its HC flag flipping to 1, and smaller than the 10-day Postgres retention
# below (so we never re-add an article that was just deleted for age).
WINDOW_DAYS = 7

# SQL Server caps the number of query parameters (~2100); fetch missing rows in
# batches well under that.
BATCH_SIZE = 500

RETENTION_INTERVAL = "10 days"


def clean_html(raw_html):
    if not raw_html:
        return ""
    if isinstance(raw_html, bytes):
        raw_html = raw_html.decode("utf-8", errors="ignore")
    soup = BeautifulSoup(raw_html, "html.parser")
    for tag in soup(["script", "style"]):
        tag.decompose()
    text = soup.get_text(separator=" ", strip=True)
    return text.encode("utf-8", errors="ignore").decode("utf-8")


def _batches(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def process_article(pg_conn, pg_cursor, article):
    """Insert one article + its chunks in a single transaction. Returns True on
    success. On any error the transaction is rolled back and the article stays
    'missing', so the next cycle retries it (no permanent skip)."""
    article_id = None
    try:
        article_id, title, url, content, ozet, ilk_cekilme_tarihi, onem_rank, kategori = article
        title = title.encode("utf-8", errors="ignore").decode("utf-8") if title else ""
        content = clean_html(content)
        ozet = clean_html(ozet)

        insert_article(pg_cursor, article_id, url, title, content, ozet, ilk_cekilme_tarihi, onem_rank, kategori)

        chunks = chunk_text(content)
        if chunks:
            embeddings = get_embeddings([f"passage: {c}" for c in chunks])
            insert_chunks(pg_cursor, article_id, url, chunks, embeddings, ilk_cekilme_tarihi, onem_rank)

        pg_conn.commit()
        return True
    except Exception as e:
        print(f"[ERROR] Failed on article ID {article_id}: {e}")
        pg_conn.rollback()
        return False


def run_cycle():
    candidate_ids = fetch_candidate_ids(WINDOW_DAYS)
    if not candidate_ids:
        print("[INFO] No eligible articles in window.")
        return

    pg_conn = connect_postgres()
    pg_cursor = pg_conn.cursor()
    try:
        # Retention: drop records older than the retention window.
        pg_cursor.execute(
            f"DELETE FROM news_chunks WHERE ilk_cekilme_tarihi < NOW() - INTERVAL '{RETENTION_INTERVAL}';"
        )
        pg_cursor.execute(
            f"DELETE FROM news_articles WHERE ilk_cekilme_tarihi < NOW() - INTERVAL '{RETENTION_INTERVAL}';"
        )
        pg_conn.commit()

        # Anti-join: process only the eligible articles not already ingested.
        existing = fetch_existing_ids(pg_cursor, candidate_ids)
        missing_ids = [i for i in candidate_ids if i not in existing]
        print(f"[INFO] {len(candidate_ids)} eligible, {len(existing)} already ingested, {len(missing_ids)} to process.")

        if not missing_ids:
            return

        inserted = 0
        for batch in _batches(missing_ids, BATCH_SIZE):
            for article in fetch_articles_by_ids(batch):
                if process_article(pg_conn, pg_cursor, article):
                    inserted += 1
            print(f"[CHECKPOINT] Inserted {inserted}/{len(missing_ids)}.")

        print(f"[INFO] Finished. Inserted {inserted}/{len(missing_ids)}.")
    finally:
        pg_cursor.close()
        pg_conn.close()


def run_worker():
    print(f"[INFO] Starting ingestion. Re-scan window: {WINDOW_DAYS} days.")
    while True:
        try:
            run_cycle()
        except Exception as e:
            print(f"[ERROR] Cycle failed: {e}")
        print(f"[INFO] Sleeping for {SLEEP_SECONDS}s...")
        time.sleep(SLEEP_SECONDS)


if __name__ == "__main__":
    run_worker()
