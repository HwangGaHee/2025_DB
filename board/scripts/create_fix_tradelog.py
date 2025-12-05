import sqlite3

con = sqlite3.connect("boardgame.db")
cur = con.cursor()

cur.execute("""
    ALTER TABLE Trade_Log
    ADD COLUMN price INTEGER;
""")

con.commit()
con.close()

print("Trade_Log 테이블에 price 컬럼 추가 완료")
