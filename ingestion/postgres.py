import psycopg2
from config import POSTGRES_CONFIG


def connect_postgres():
    return psycopg2.connect(
        host=POSTGRES_CONFIG["host"],
        database=POSTGRES_CONFIG["database"],
        user=POSTGRES_CONFIG["username"],
        password=POSTGRES_CONFIG["password"],
    )


def insert_article(pg_cursor, article_id, url, title, full_text, ilk_cekilme_tarihi):
    pg_cursor.execute("""
        INSERT INTO news_articles (article_id, article_url, title, full_text, ilk_cekilme_tarihi)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (article_id) DO NOTHING
    """, (article_id, url, title, full_text, ilk_cekilme_tarihi))


def insert_chunks(pg_cursor, article_id, url, chunks, embeddings, ilk_cekilme_tarihi):
    for idx, (chunk_text, emb) in enumerate(zip(chunks, embeddings)):
        pg_cursor.execute("""
            INSERT INTO news_chunks (article_id, article_url, chunk_index, chunk_text, embedding, ilk_cekilme_tarihi)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (article_id, url, idx, chunk_text, emb.tolist(), ilk_cekilme_tarihi))