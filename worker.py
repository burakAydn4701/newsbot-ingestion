# -*- coding: utf-8 -*-
import time
import re
from bs4 import BeautifulSoup
from ingestion.sql_server import fetch_new_articles
from ingestion.postgres import connect_postgres, insert_article, insert_chunks
from ingestion.chunks import chunk_text
from ingestion.embeddings import get_embeddings
from config import SLEEP_SECONDS, CHECKPOINT_SIZE


def clean_html(raw_html):
    soup = BeautifulSoup(raw_html, "html.parser")
    for tag in soup(["script", "style"]):
        tag.decompose()
    return soup.get_text(separator=" ", strip=True)


def load_last_id(pg_cursor):
    pg_cursor.execute("SELECT last_id FROM ingestion_state WHERE name='news';")
    row = pg_cursor.fetchone()
    return row[0] if row else 0


def save_last_id(pg_cursor, last_id):
    pg_cursor.execute("""
        INSERT INTO ingestion_state(name, last_id)
        VALUES('news', %s)
        ON CONFLICT(name) DO UPDATE SET last_id = EXCLUDED.last_id
    """, (last_id,))


def run_worker():
    pg_conn = connect_postgres()
    pg_cursor = pg_conn.cursor()

    pg_cursor.execute("""
        CREATE TABLE IF NOT EXISTS ingestion_state (
            name TEXT PRIMARY KEY,
            last_id BIGINT
        )
    """)
    pg_conn.commit()

    last_id = load_last_id(pg_cursor)
    print(f"[INFO] Starting ingestion. Last processed ID: {last_id}")

    while True:
        # Drop chunks and articles older than 10 days based on ilk_cekilme_tarihi
        pg_cursor.execute("DELETE FROM news_chunks WHERE ilk_cekilme_tarihi < NOW() - INTERVAL '10 days';")
        pg_cursor.execute("DELETE FROM news_articles WHERE ilk_cekilme_tarihi < NOW() - INTERVAL '10 days';")
        deleted_count = pg_cursor.rowcount
        pg_conn.commit()
        if deleted_count > 0:
            print(f"[INFO] Dropped {deleted_count} old articles.")

        new_articles = fetch_new_articles(last_id)
        total_articles = len(new_articles)
        print(f"[INFO] Found {total_articles} new articles.")

        if not new_articles:
            print(f"[INFO] No new articles. Sleeping for {SLEEP_SECONDS}s...")
            time.sleep(SLEEP_SECONDS)
            continue

        inserted_count = 0

        for article in new_articles:
            article_id = None
            try:
                article_id, title, url, content, ilk_cekilme_tarihi = article
                content = clean_html(content)
                insert_article(pg_cursor, article_id, url, title, content, ilk_cekilme_tarihi)
                chunks = chunk_text(content)
                embeddings = get_embeddings(chunks)
                insert_chunks(pg_cursor, article_id, url, chunks, embeddings, ilk_cekilme_tarihi)

                last_id = max(last_id, article_id)
                save_last_id(pg_cursor, last_id)
                pg_conn.commit()
                inserted_count += 1

                if inserted_count % CHECKPOINT_SIZE == 0:
                    print(f"[CHECKPOINT] Inserted {inserted_count}/{total_articles} articles. Last processed ID: {last_id}")

            except Exception as e:
                print(f"[ERROR] Failed on article ID {article_id}: {e}")
                pg_conn.rollback()
                continue

        print(f"[INFO] Finished inserting {inserted_count}/{total_articles} articles. Last processed ID: {last_id}. Sleeping for {SLEEP_SECONDS}s...")
        time.sleep(SLEEP_SECONDS)


if __name__ == "__main__":
    run_worker()