import sqlite3

con = sqlite3.connect("boardgame.db")
cur = con.cursor()

games = [
    ("Catan", "Strategy", 3, 4, 60, 3.0),
    ("Splendor", "Family", 2, 4, 30, 1.5),
    ("Azul", "Abstract", 2, 4, 40, 1.8),
    ("7 Wonders", "Card", 3, 7, 45, 2.5)
]

for g in games:
    cur.execute("""
        INSERT INTO BoardGame_Master (title, genre, min_players, max_players, avg_playtime, difficulty)
        VALUES (?, ?, ?, ?, ?, ?)
    """, g)

con.commit()
con.close()

print("기본 게임 4개 등록 완료!")
