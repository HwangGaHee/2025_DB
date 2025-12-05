import sqlite3

# boardgame.db에 있는 Market_Listing 테이블에
# buyer_id 컬럼을 추가하는 스크립트입니다.
# ★ 이 파일은 한 번만 실행하면 됩니다.

def add_buyer_column():
    con = sqlite3.connect("boardgame.db")
    cur = con.cursor()

    print("➡ Market_Listing 테이블 구조 변경 시작")

    # 이미 buyer_id가 있는지 한 번 확인 (있으면 건너뜀)
    cur.execute("PRAGMA table_info(Market_Listing);")
    cols = [row[1] for row in cur.fetchall()]

    if "buyer_id" in cols:
        print("✅ 이미 buyer_id 컬럼이 존재합니다. 작업을 건너뜁니다.")
        con.close()
        return

    # 없으면 컬럼 추가
    cur.execute("ALTER TABLE Market_Listing ADD COLUMN buyer_id INT;")

    con.commit()
    con.close()
    print("✅ buyer_id 컬럼 추가 완료!")




if __name__ == "__main__":
    add_buyer_column()
