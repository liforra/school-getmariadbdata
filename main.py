import mariadb
import sys

# Connect to MariaDB Platform
try:
    conn = mariadb.connect(
        user="root",
        password="Passwort123",
        host="172.20.10.4",
        port=3306,
        database="test",
    )
except mariadb.Error as e:
    print(f"Error connecting to MariaDB Platform: {e}")
    sys.exit(1)

# Get Cursor
cur = conn.cursor()

cur.execute("SELECT id, name, randomnumber FROM names WHERE name=?", ("Alex1",))
cur.execute("SELECT id, name, randomnumber FROM names")


for (id, name, randomnum) in cur:
    print(f"ID: {id}, Name: {name}, Random Number: {randomnum}")