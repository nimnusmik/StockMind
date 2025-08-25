import psycopg2

def get_db_connection():
    return psycopg2.connect(
        dbname="stockmind",
        user="user",
        password="password",
        host="db",
        port="5432"
    )