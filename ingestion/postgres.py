import psycopg2
from config import POSTGRES_CONFIG


def connect_postgres():
    return psycopg2.connect(
        host=POSTGRES_CONFIG["host"],
        database=POSTGRES_CONFIG["database"],
        user=POSTGRES_CONFIG["username"],
        password=POSTGRES_CONFIG["password"],
    )


def fetch_existing_ids(pg_cursor, ids):
    """Which of the given article ids are already ingested (present in
    news_articles). The worker processes only the ones NOT returned here."""
    if not ids:
        return set()
    pg_cursor.execute(
        "SELECT article_id FROM news_articles WHERE article_id = ANY(%s)",
        (list(ids),),
    )
    return {r[0] for r in pg_cursor.fetchall()}


def insert_article(pg_cursor, article_id, url, title, full_text, ozet, ilk_cekilme_tarihi, onem_rank=None, kategori=None):
    pg_cursor.execute("""
        INSERT INTO news_articles (article_id, article_url, title, full_text, ozet, ilk_cekilme_tarihi, onem_rank, category)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (article_id) DO NOTHING
    """, (article_id, url, title, full_text, ozet, ilk_cekilme_tarihi, onem_rank, kategori))


def insert_chunks(pg_cursor, article_id, url, chunks, embeddings, ilk_cekilme_tarihi, onem_rank=None):
    for idx, (chunk_text, emb) in enumerate(zip(chunks, embeddings)):
        pg_cursor.execute("""
            INSERT INTO news_chunks (article_id, article_url, chunk_index, chunk_text, embedding, ilk_cekilme_tarihi, onem_rank)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (article_id, url, idx, chunk_text, emb.tolist(), ilk_cekilme_tarihi, onem_rank))