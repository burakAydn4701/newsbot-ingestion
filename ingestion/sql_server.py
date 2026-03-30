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


def fetch_new_articles(last_id):
    conn = connect_sql_server()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, baslik, orijinal_URL, metin, ozet, ilk_cekilme_tarihi, onem_rank, kategori
        FROM dbo.haberler WITH (NOLOCK)
        WHERE id > ? AND HC = 1
        ORDER BY id
    """, (last_id,))
    articles = cursor.fetchall()
    conn.close()
    return articles