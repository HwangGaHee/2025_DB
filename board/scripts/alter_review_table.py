import sqlite3

def upgrade_review_table():

    con = sqlite3.connect("boardgame.db")
    cur = con.cursor()

    print("➡ Review 테이블 구조 확인 중...")

    cur.execute("PRAGMA table_info(Review)")
    columns = [row[1] for row in cur.fetchall()]

    if "review_type" not in columns:
        cur.execute("ALTER TABLE Review ADD COLUMN review_type TEXT;")
        print("✅ review_type 컬럼 추가")

    if "related_id" not in columns:
        cur.execute("ALTER TABLE Review ADD COLUMN related_id INTEGER;")
        print("✅ related_id 컬럼 추가")

    if "rating_type" not in columns:
        cur.execute("ALTER TABLE Review ADD COLUMN rating_type TEXT;")
        print("✅ rating_type 컬럼 추가")

    if "target_id" not in columns:
        cur.execute("ALTER TABLE Review ADD COLUMN target_id INTEGER;")
        print("✅ target_id 컬럼 추가")

    con.commit()
    con.close()
    print("✅ Review 테이블 업그레이드 완료")

if __name__ == "__main__":
    upgrade_review_table()
