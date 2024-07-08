import psycopg2

def top_coins(conn):
    cur = conn.cursor()
    query = """
    SELECT id, cg_id
    FROM top_coins_info
    ORDER BY id
    LIMIT 100;
    """
    cur.execute(query)
    top_coins = cur.fetchall()
    cur.close()
    return top_coins