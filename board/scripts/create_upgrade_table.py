import sqlite3

con = sqlite3.connect("boardgame.db")
cur = con.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS Role_Request (
    request_id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    current_role TEXT NOT NULL,
    request_role TEXT NOT NULL,
    request_date TEXT DEFAULT CURRENT_TIMESTAMP,
    status TEXT DEFAULT 'Pending'
)
""")

con.commit()
con.close()

print("✅ Role_Request 테이블 생성 완료")
