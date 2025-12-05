import sqlite3

con = sqlite3.connect("boardgame.db")
cur = con.cursor()

cur.execute("""
CREATE TABLE Review (
    review_id INTEGER PRIMARY KEY AUTOINCREMENT,
    writer_id INTEGER NOT NULL,
    target_user INTEGER,
    meeting_id INTEGER,
    trade_id INTEGER,
    mode TEXT NOT NULL,         -- 'trade' 또는 'meeting'
    rating INTEGER NOT NULL,    -- 1~5
    content TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (writer_id) REFERENCES User(user_id),
    FOREIGN KEY (target_user) REFERENCES User(user_id),
    FOREIGN KEY (meeting_id) REFERENCES Gathering(meeting_id),
    FOREIGN KEY (trade_id) REFERENCES Trade_Log(trade_id)
);
""")

con.commit()
con.close()

print("Review 테이블 새로 생성 완료!")