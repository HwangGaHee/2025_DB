import sqlite3

con = sqlite3.connect("boardgame.db")
cur = con.cursor()

meetings = [
    (1, "부산 카탄 모임", "부산진구", "2025-12-01 18:00", 4, 1, "모집중"),
    (1, "서면 스플렌더 모임", "서면", "2025-12-05 20:00", 4, 2, "모집중"),
    (1, "양산 7원더스 모임", "양산", "2025-12-03 19:30", 7, 3, "모집중")
]

for m in meetings:
    cur.execute("""
        INSERT INTO Gathering (host_id, title, location, meet_date, max_participants, current_participants, status)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, m)

con.commit()
con.close()

print("테스트용 모임 3개 등록 완료!")
