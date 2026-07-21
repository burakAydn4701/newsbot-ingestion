import pyodbc
from config import SQL_SERVER_CONFIG


def connect_sql_server():
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={SQL_SERVER_CONFIG['server']};"
        f"DATABASE={SQL_SERVER_CONFIG['database']};"
        f"UID={SQL_SERVER_CONFIG['username']};"
        f"PWD={SQL_SERVER_CONFIG['password']};"
        f"Encrypt=no;"
    )
    return pyodbc.connect(conn_str)


def fetch_candidate_ids(window_days):
    """IDs of all currently-eligible (HC=1) articles within the recent window.
    Lightweight (ids only) — the worker diffs these against what's already in
    Postgres and only fetches full content for the missing ones. Using a window
    + anti-join instead of an id watermark is what fixes articles whose HC flag
    flips after the worker has already passed their id."""
    conn = connect_sql_server()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id
        FROM dbo.haberler WITH (NOLOCK)
        WHERE HC = 1
          AND ilk_cekilme_tarihi >= DATEADD(DAY, ?, GETDATE())
    """, (-window_days,))
    ids = [r[0] for r in cursor.fetchall()]
    conn.close()
    return ids


def fetch_articles_by_ids(ids):
    """Full rows for the given article ids (SQL Server caps parameters, so the
    worker calls this in batches)."""
    if not ids:
        return []
    conn = connect_sql_server()
    cursor = conn.cursor()
    placeholders = ",".join("?" for _ in ids)
    cursor.execute(f"""
        SELECT id, baslik, orijinal_URL, metin, ozet, ilk_cekilme_tarihi, onem_rank, kategori
        FROM dbo.haberler WITH (NOLOCK)
        WHERE id IN ({placeholders})
        ORDER BY id
    """, list(ids))
    rows = cursor.fetchall()
    conn.close()
    return rows