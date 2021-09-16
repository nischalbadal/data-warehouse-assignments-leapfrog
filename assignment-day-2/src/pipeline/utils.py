import psycopg2

def connect():
    return psycopg2.connect(
        host = 'localhost',
        database = 'data-internship',
        user='postgres',
        password='nischal541',
        port = 5432
    )