import sqlite3

con = sqlite3.connect("boardgame.db")
cur = con.cursor()

cur.execute("""
INSERT INTO User (username, password_hash, role)
VALUES ('test', '1234', 'User');
""")

con.commit()
con.close()

print("Inserted!")
