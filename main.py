import psycopg2
import os
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

conn = psycopg2.connect(
    host="localhost",
    port="5432",
    user="admin",
    database="postgres",
    password=os.getenv("PGSQL_PASSWORD")
)
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

try:
    cursor = conn.cursor()
    print("drop table if already exist")
    cursor.execute("DROP DATABASE IF EXISTS s2_pu_bda")
    cursor.execute("CREATE DATABASE s2_pu_bda")
    print("create database first")
    conn.commit()
except (Exception, psycopg2.Error) as error:
    print("error pas konek ke PostgreSQL", error)
finally:
    if (conn):
        cursor.close()
        conn.close()
        print("PostgreSQL connection harus di closed biar ga buang resource")

# have to create another connection,
# creating database in postgres cannot be exuected in one trancation block with other execution
# that supposed to run inside that newly created database
conn = psycopg2.connect(
    host="localhost",
    port="5432",
    user="admin",
    database="s2_pu_bda",
    password=os.getenv("PGSQL_PASSWORD")
)
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
try:
    print("create table customers and do insertions")
    cursor = conn.cursor()
    cursor.execute(
        "CREATE TABLE customers (name VARCHAR(100), address VARCHAR(200))")
    sql = "INSERT INTO customers (name, address) VALUES (%s, %s)"
    val = ("Abdi", "In the Road")
    cursor.execute(sql, val)
    conn.commit()
except (Exception, psycopg2.Error) as error:
    print("error pas konek ke PostgreSQL", error)
finally:
    if (conn):
        cursor.close()
        conn.close()
        print("PostgreSQL connection harus di closed biar ga buang resource")
