import sqlite3

con = sqlite3.connect("boardgame.db")
cur = con.cursor()

cur.execute("DROP TABLE IF EXISTS Review;")

con.commit()
con.close()

print("Review 테이블 삭제 완료!")
