import sqlite3

# ================================
# 회원가입
# ================================


def sign_up():
    con = sqlite3.connect("boardgame.db")
    cur = con.cursor()

    print("\n=== 회원가입 ===")
    username = input("새 ID 입력: ")
    password = input("새 비밀번호 입력: ")
    location = input("사는 지역(선택): ")

    cur.execute("SELECT username FROM User WHERE username=?", (username,))
    if cur.fetchone():
        print("❌ 이미 존재하는 ID입니다.")
        con.close()
        return

    cur.execute("""
        INSERT INTO User (username, password_hash, location_info, role)
        VALUES (?, ?, ?, 'User')
    """, (username, password, location))

    con.commit()
    con.close()
    print("✅ 회원가입 완료!")

# ================================
# 로그인
# ================================


def login():
    con = sqlite3.connect("boardgame.db")
    cur = con.cursor()

    username = input("ID: ")
    pw = input("Password: ")

    cur.execute("""
        SELECT user_id, role FROM User
        WHERE username=? AND password_hash=?
    """, (username, pw))
    row = cur.fetchone()

    con.close()

    if not row:
        print("❌ 로그인 실패")
        return None

    print(f"✅ 로그인 성공! {row[1]} 계정")
    return row[0], row[1]

# ================================
# 메인 메뉴
# ================================


def user_menu(user_id):
    while True:
        print("\n=== User Menu ===")
        print("1. 보드게임 등록")
        print("2. 보드게임 추천")
        print("3. 모임 검색 및 참여")
        print("4. 모임 개설")
        print("5. 내가 주최한 모임 신청 승인")
        print("6. 중고거래 등록")
        print("7. 중고거래 이용")
        print("8. 판매자 거래 승인")
        print("9. 내 보드게임 목록 보기")
        print("10. 후기 작성")
        print("11. 내 평판 보기")
        print("12. 등급 신청")
        print("13. 거래 진행 (계좌/주소 입력)")
        print("14. 거래 완료 처리")



        print("0. 로그아웃")

        choice = input("선택: ")

        if choice == "1":
            register_game(user_id)
        elif choice == "2":
            recommend_games()
        elif choice == "3":
            search_gatherings()
            join_gathering(user_id)
        elif choice == "4":
            create_gathering(user_id)

        elif choice == "5":
            approve_gathering_requests(user_id)
        elif choice == "6":
            register_sale(user_id)
        elif choice == "7":
            start_market(user_id)
        elif choice == "8":
            approve_trade(user_id)
        elif choice == "9":
            my_games(user_id)
        elif choice == "10":
            write_review(user_id)
        elif choice == "11":
            view_my_reputation(user_id)
        elif choice == "12":
            request_role_upgrade(user_id)
        elif choice == "13":
            exchange_payment_info(user_id)
        elif choice == "14":
            complete_trade(user_id)



        elif choice == "0":
            print("로그아웃합니다.")
            break
        else:
            print("❌ 잘못된 입력입니다.")

# ================================
# 시스템 시작
# ================================


def start():
    print("=== BoardGame Community System ===")

    while True:
        print("\n1. 로그인")
        print("2. 회원가입")
        print("0. 종료")
        choice = input("선택: ")

        if choice == "1":
            result = login()
            if result:
                user_id, role = result
                # Admin 계정이면 관리자 메뉴로
                if role == "Admin":
                    admin_menu()
                else:
                    user_menu(user_id)
        elif choice == "2":
            sign_up()
        elif choice == "0":
            print("종료합니다.")
            break
        else:
            print("❌ 잘못된 입력입니다.")

# ================================
# 보드게임 등록
# ================================


def register_game(user_id):
    con = sqlite3.connect("boardgame.db")
    cur = con.cursor()

    print("\n=== 보드게임 등록 ===")
    title = input("게임 이름: ")

    cur.execute("SELECT game_id FROM BoardGame_Master WHERE title=?", (title,))
    row = cur.fetchone()

    if row:
        game_id = row[0]
        print(f"📌 기존 게임 game_id={game_id}")
    else:
        genre = input("장르: ")
        min_p = input("최소 인원: ")
        max_p = input("최대 인원: ")
        playtime = input("평균 플레이 시간: ")
        diff = input("난이도: ")

        cur.execute("""
            INSERT INTO BoardGame_Master 
            (title, genre, min_players, max_players, avg_playtime, difficulty)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (title, genre, min_p, max_p, playtime, diff))

        con.commit()
        cur.execute("SELECT last_insert_rowid()")
        game_id = cur.fetchone()[0]

    cond = input("게임 상태(A/B/C): ").upper()
    if cond not in ["A", "B", "C"]:
        cond = "A"

    cur.execute("""
        INSERT INTO User_Collection (owner_id, game_id, condition_rank)
        VALUES (?, ?, ?)
    """, (user_id, game_id, cond))

    con.commit()
    con.close()
    print("✅ 보드게임 등록 완료!")

# ================================
# 추천
# ================================


def recommend_games():
    con = sqlite3.connect("boardgame.db")
    cur = con.cursor()

    genre = input("장르(엔터=전체): ")
    players = input("플레이 인원(엔터=전체): ")
    max_diff = input("최대 난이도(엔터=5): ")

    players = None if not players else int(players)
    max_diff = 5.0 if not max_diff else float(max_diff)

    query = """
        SELECT title, genre, min_players, max_players, avg_playtime, difficulty
        FROM BoardGame_Master
        WHERE difficulty <= ?
    """

    params = [max_diff]

    if genre:
        query += " AND genre LIKE ?"
        params.append('%'+genre+'%')

    if players:
        query += " AND min_players <= ? AND max_players >= ?"
        params += [players, players]

    query += " ORDER BY difficulty ASC"

    cur.execute(query, params)

    for r in cur.fetchall():
        print(f"- {r[0]} | {r[1]} | {r[2]}~{r[3]} | {r[4]}분 | 난이도:{r[5]}")

    con.close()

# ================================
# 모임 검색
# ================================


def search_gatherings():
    con = sqlite3.connect("boardgame.db")
    cur = con.cursor()

    loc = input("지역(엔터=전체): ")
    date = input("날짜(YYYY-MM-DD, 엔터=전체): ")
    min_p = input("최소 모집 인원(엔터=전체): ")

    query = """
        SELECT meeting_id, title, location, meet_date,
               max_participants, current_participants
        FROM Gathering
        WHERE 1=1
    """
    params = []

    if loc:
        query += " AND location LIKE ?"
        params.append('%'+loc+'%')

    if date:
        query += " AND date(meet_date)=?"
        params.append(date)

    if min_p:
        query += " AND max_participants >= ?"
        params.append(int(min_p))

    cur.execute(query, params)

    for r in cur.fetchall():
        print(f"[{r[0]}] {r[1]} | {r[2]} | {r[3]} | {r[5]}/{r[4]}명")

    con.close()

# ================================
# 모임 참여
# ================================


def join_gathering(user_id):
    con = sqlite3.connect("boardgame.db")
    cur = con.cursor()

    meeting_id = input("참여할 모임 ID: ")

    try:
        con.execute("BEGIN")

        # 1) 모임 정보 확인
        cur.execute("""
            SELECT max_participants, current_participants, host_id
            FROM Gathering
            WHERE meeting_id=?
        """, (meeting_id,))
        row = cur.fetchone()
        if not row:
            print("❌ 모임 없음")
            con.rollback()
            return

        max_p, cur_p, host_id = row

        # 2) 이미 신청했는지 확인
        cur.execute("""
            SELECT status
            FROM Gathering_Participants
            WHERE meeting_id=? AND user_id=?
        """, (meeting_id, user_id))
        if cur.fetchone():
            print("❌ 이미 이 모임에 신청했습니다.")
            con.rollback()
            return

        # 3) 내 등급 확인 (VIP 여부)
        cur.execute("SELECT role FROM User WHERE user_id=?", (user_id,))
        role_row = cur.fetchone()
        user_role = role_row[0] if role_row else "User"

        # 4) 대기열 순서 계산
        # ❗ BadUser는 항상 맨 마지막 순번
        if user_role == "BadUser":
            cur.execute("""
                SELECT COALESCE(MAX(wait_order), 0)
                FROM Gathering_Participants
                WHERE meeting_id=? AND status='Waitlist'
            """, (meeting_id,))
            max_order = cur.fetchone()[0]
            my_order = max_order + 1
            print("🚫 BadUser → 대기열 최하 순번으로 배치됩니다.")

        elif user_role == "VIP":
            # 기존 대기자들 순번 뒤로 한 칸씩 밀기
            cur.execute("""
                UPDATE Gathering_Participants
                SET wait_order = wait_order + 1
                WHERE meeting_id=?
                  AND status = 'Waitlist'
            """, (meeting_id,))
            my_order = 1
            print("⭐ VIP → 대기열 맨 앞에 등록됩니다.")
        else:
            # 일반 유저: 현재 대기열에서 가장 큰 순번 + 1
            cur.execute("""
                SELECT COALESCE(MAX(wait_order), 0)
                FROM Gathering_Participants
                WHERE meeting_id=? AND status='Waitlist'
            """, (meeting_id,))
            max_order = cur.fetchone()[0]
            my_order = max_order + 1
            print(f"⏳ 현재 대기순번: {my_order}번")

        # 5) 대기 상태로만 기록 (Approved 아님!)
        cur.execute("""
            INSERT INTO Gathering_Participants (meeting_id, user_id, status, wait_order)
            VALUES (?, ?, 'Waitlist', ?)
        """, (meeting_id, user_id, my_order))

        con.commit()
        print("✅ 모임 대기열에 등록되었습니다. (호스트 승인 후 최종 참가)")

    except Exception as e:
        con.rollback()
        print("❌ 오류:", e)

    finally:
        con.close()


def create_gathering(user_id):
    con = sqlite3.connect("boardgame.db")
    cur = con.cursor()

    print("\n=== 🗓 모임 개설 ===")

    title = input("모임 제목: ")
    desc = input("모임 한줄 설명: ")
    location = input("지역: ")
    date = input("날짜 (YYYY-MM-DD HH:MM): ")
    max_p = input("최대 인원: ")

    try:
        cur.execute("""
            INSERT INTO Gathering
            (title, description, location, meet_date,
             max_participants, current_participants,
             host_id, status)
            VALUES (?, ?, ?, ?, ?, 0, ?, 'Open')
        """, (
            title,
            desc,
            location,
            date,
            max_p,
            user_id
        ))

        con.commit()
        print("✅ 모임이 개설되었습니다!")

    except Exception as e:
        print("❌ 모임 생성 실패:", e)

    con.close()


def approve_gathering_requests(user_id):
    con = sqlite3.connect("boardgame.db")
    cur = con.cursor()

    print("\n=== 🧑‍💼 모임 참가 신청 승인 (호스트 전용) ===")

    # 1) 내가 호스트이면서, 대기자가 있는 모임 목록
    cur.execute("""
        SELECT DISTINCT
            G.meeting_id,
            G.title,
            G.location,
            G.meet_date
        FROM Gathering G
        JOIN Gathering_Participants GP
             ON G.meeting_id = GP.meeting_id
        WHERE G.host_id = ?
          AND GP.status = 'Waitlist'
        ORDER BY G.meet_date ASC
    """, (user_id,))

    meetings = cur.fetchall()
    if not meetings:
        print("📌 대기 중인 신청이 있는 모임이 없습니다.")
        con.close()
        return

    print("\n📋 승인 대기 모임 목록:")
    for m in meetings:
        print(f"[{m[0]}] {m[1]} | {m[2]} | {m[3]}")

    meeting_id = input("\n승인할 모임 ID 선택 (0=취소): ")
    if meeting_id == "0":
        con.close()
        return

    # 2) 선택한 모임의 대기열 조회 (VIP가 위로 오도록 wait_order 순)
    cur.execute("""
        SELECT
            GP.user_id,
            U.username,
            GP.wait_order
        FROM Gathering_Participants GP
        JOIN User U ON GP.user_id = U.user_id
        WHERE GP.meeting_id = ?
          AND GP.status = 'Waitlist'
        ORDER BY GP.wait_order ASC
    """, (meeting_id,))

    wait_rows = cur.fetchall()
    if not wait_rows:
        print("📌 이 모임에는 대기자가 없습니다.")
        con.close()
        return

    print("\n📋 대기열 목록 (번호가 작을수록 먼저 신청됨 / VIP는 맨 위로):")
    for w in wait_rows:
        print(f"- user_id:{w[0]} | ID:{w[1]} | 대기순번:{w[2]}")

    # 3) 승인할 유저 선택
    target_user = input("\n승인할 user_id 입력 (0=취소): ")
    if target_user == "0":
        con.close()
        return

    # 4) 정원 확인
    cur.execute("""
        SELECT max_participants, current_participants
        FROM Gathering
        WHERE meeting_id = ?
    """, (meeting_id,))
    g = cur.fetchone()
    if not g:
        print("❌ 모임 정보가 없습니다.")
        con.close()
        return

    max_p, cur_p = g

    if cur_p >= max_p:
        print("❌ 정원이 이미 가득 찼습니다. 더 이상 승인할 수 없습니다.")
        con.close()
        return

    try:
        con.execute("BEGIN")

        # 5) 선택한 사람 Approved 로 변경
        cur.execute("""
            UPDATE Gathering_Participants
            SET status = 'Approved'
            WHERE meeting_id = ?
              AND user_id = ?
              AND status = 'Waitlist'
        """, (meeting_id, target_user))

        if cur.rowcount == 0:
            print("❌ 해당 유저는 대기열에 없거나 이미 처리되었습니다.")
            con.rollback()
            con.close()
            return

        # 6) 인원 수 +1
        cur.execute("""
            UPDATE Gathering
            SET current_participants = current_participants + 1
            WHERE meeting_id = ?
        """, (meeting_id,))

        con.commit()
        print("✅ 참가 승인 완료!")

    except Exception as e:
        con.rollback()
        print("❌ 승인 처리 중 오류:", e)

    finally:
        con.close()

# ================================
# 중고거래 등록
# ================================


def register_sale(user_id):
    con = sqlite3.connect("boardgame.db")
    cur = con.cursor()

    # 🔒 BadUser 판매 제한
    cur.execute("SELECT role FROM User WHERE user_id=?", (user_id,))
    role = cur.fetchone()[0]

    if role == "BadUser":
        print("❌ BadUser 계정은 중고거래 판매 등록이 제한됩니다.")
        con.close()
        return

    cur.execute("""
        SELECT UC.collection_id, BM.title, UC.condition_rank
        FROM User_Collection UC
        JOIN BoardGame_Master BM ON UC.game_id=BM.game_id
        WHERE UC.owner_id=? AND UC.status='Available'
    """, (user_id,))

    rows = cur.fetchall()

    if not rows:
        print("❌ 판매 가능 게임 없음")
        con.close()
        return

    for r in rows:
        print(f"{r[0]} | {r[1]} | 상태:{r[2]}")

    col_id = input("판매할 collection_id: ")

    price = input("가격: ")
    desc = input("설명: ")

    cur.execute("""
        INSERT INTO Market_Listing
        (collection_id, seller_id, price, description)
        VALUES (?, ?, ?, ?)
    """, (col_id, user_id, price, desc))

    cur.execute("""
        UPDATE User_Collection
        SET status='In_Trade'
        WHERE collection_id=?
    """, (col_id,))

    con.commit()
    con.close()
    print("✅ 판매 등록 완료")

# ================================
# 거래 메뉴
# ================================


def start_market(user_id):

    print("\n=== 중고거래 메뉴 ===")
    print("1. 판매 리스트 보기")
    print("2. 검색하기")
    choice = input("선택: ")

    if choice == "1":
        show_market(user_id, "list")
    elif choice == "2":
        show_market(user_id, "search")
    else:
        print("❌ 잘못된 선택")

# ================================
# 공통 거래 화면
# ================================


def show_market(user_id, mode):
    con = sqlite3.connect("boardgame.db")
    cur = con.cursor()

    query = """
        SELECT ML.listing_id, BM.title, BM.genre,
               UC.condition_rank,
               ML.price, ML.description,
               U.username, ML.seller_id
        FROM Market_Listing ML
        JOIN User_Collection UC ON ML.collection_id=UC.collection_id
        JOIN BoardGame_Master BM ON UC.game_id=BM.game_id
        JOIN User U ON ML.seller_id=U.user_id
        WHERE UC.status='In_Trade'
    """

    params = []

    if mode == "search":
        title = input("검색 이름: ")
        if title:
            query += " AND BM.title LIKE ?"
            params.append("%"+title+"%")

    # 🔽 정렬 조건 추가: VIP 판매자 글이 위로 오도록
    query += """
        ORDER BY
            CASE
                WHEN U.role = 'VIP' THEN 1
                ELSE 2
            END,
            ML.listing_id DESC
    """

    cur.execute(query, params)

    rows = cur.fetchall()

    if not rows:
        print("❌ 목록 없음")
        con.close()
        return

    for r in rows:
        print(f"[{r[0]}] {r[1]} | {r[2]} | {r[3]} | {r[4]}원 | 판매자:{r[6]}")

    select_id = input("구매 신청할 리스트ID (0=취소): ")

    if select_id == "0":
        con.close()
        return

    cur.execute("""
        UPDATE Market_Listing
        SET buyer_id=?, status='Requested'
        WHERE listing_id=?
    """, (user_id, select_id))

    con.commit()
    con.close()

    print("✅ 구매 신청 완료!")

# ================================
# 판매자 승인
# ================================


def approve_trade(user_id):
    con = sqlite3.connect("boardgame.db")
    cur = con.cursor()

    cur.execute("""
        SELECT
            ML.listing_id,
            BM.title,
            ML.buyer_id,
            ML.price
        FROM Market_Listing ML
        JOIN User_Collection UC ON ML.collection_id = UC.collection_id
        JOIN BoardGame_Master BM ON UC.game_id = BM.game_id
        WHERE ML.seller_id = ?
          AND ML.status = 'Requested'
    """, (user_id,))

    rows = cur.fetchall()

    if not rows:
        print("📌 승인 대기중인 요청 없음")
        con.close()
        return

    print("\n=== 승인 대기 목록 ===")
    for r in rows:
        print(f"[{r[0]}] {r[1]} | 구매자:{r[2]} | {r[3]}원")

    listing_id = input("\n승인할 리스트 ID: ")

    cur.execute("""
        UPDATE Market_Listing
        SET status='Approved'
        WHERE listing_id=?
          AND seller_id=?
          AND status='Requested'
    """, (listing_id, user_id))

    if cur.rowcount == 0:
        print("❌ 승인 실패 (ID 오류 또는 이미 처리됨)")
    else:
        print("✅ 판매자가 승인했습니다! 계좌 교환 단계로 이동합니다.")

    con.commit()
    con.close()



def exchange_payment_info(user_id):
    con = sqlite3.connect("boardgame.db")
    cur = con.cursor()

    cur.execute("""
        SELECT listing_id, seller_id, buyer_id, status
        FROM Market_Listing
        WHERE status='Approved'
          AND (seller_id=? OR buyer_id=?)
    """, (user_id,user_id))

    rows = cur.fetchall()

    if not rows:
        print("📌 진행 중인 거래 없음")
        con.close()
        return

    for r in rows:
        print(f"[{r[0]}] 거래 상태: {r[3]}")

    lid = input("진행할 거래 ID: ")

    cur.execute("""
        SELECT seller_id, buyer_id
        FROM Market_Listing
        WHERE listing_id=?
    """,(lid,))

    seller_id,buyer_id = cur.fetchone()

    if user_id == seller_id:
        acc = input("내 계좌번호 입력: ")
        cur.execute("""
            UPDATE Market_Listing SET seller_account=?
            WHERE listing_id=?
        """,(acc,lid))

    elif user_id == buyer_id:
        addr = input("배송주소 입력: ")
        cur.execute("""
            UPDATE Market_Listing SET buyer_address=?
            WHERE listing_id=?
        """,(addr,lid))

    else:
        print("❌ 권한 없음")
        con.close()
        return

    # 계좌 & 주소 둘 다 있으면 결제완료 상태로 전환
    cur.execute("""
        SELECT seller_account,buyer_address
        FROM Market_Listing WHERE listing_id=?
    """,(lid,))

    acc,addr = cur.fetchone()

    if acc and addr:
        cur.execute("""
            UPDATE Market_Listing SET status='Paid'
            WHERE listing_id=?
        """,(lid,))
        print("✅ 정보 교환 완료 → 입금 단계 돌입")

    con.commit()
    con.close()


# ================================
# 내 보드게임 목록
# ================================


def my_games(user_id):

    con = sqlite3.connect("boardgame.db")
    cur = con.cursor()

    print("\n=== 🎮 내가 가진 보드게임 목록 ===")

    cur.execute("""
        SELECT UC.collection_id,
               BM.title,
               BM.genre,
               UC.condition_rank,
               UC.status
        FROM User_Collection UC
        JOIN BoardGame_Master BM ON UC.game_id = BM.game_id
        WHERE UC.owner_id = ?
    """, (user_id,))

    rows = cur.fetchall()

    if not rows:
        print("❌ 보유 중인 게임이 없습니다.")
        con.close()
        return

    for r in rows:
        print(f"[{r[0]}] {r[1]} | {r[2]} | 상태:{r[3]} | 거래상태:{r[4]}")

    con.close()

# ================================
# 후기 작성 (메뉴)
# ================================


def write_review(user_id):
    while True:
        print("\n=== 후기 작성 ===")
        print("1. 거래 후기 작성")
        print("2. 모임 후기 작성")
        print("0. 돌아가기")
        choice = input("선택: ")

        if choice == "1":
            write_trade_review(user_id)
        elif choice == "2":
            write_event_review(user_id)
        elif choice == "0":
            return
        else:
            print("❌ 잘못된 입력입니다.")

# ================================
# 거래 후기 작성
# ================================


def write_trade_review(user_id):
    con = sqlite3.connect("boardgame.db")
    cur = con.cursor()

    print("\n=== 🧾 거래 후기 작성 ===")

    # 내가 참여한 거래 중, 아직 내가 리뷰 안 쓴 것만 가져오기
    cur.execute("""
        SELECT
            TL.trade_id,
            CASE
                WHEN TL.seller_id = ? THEN TL.buyer_id
                ELSE TL.seller_id
            END AS target_user_id,
            U.username AS target_username,
            TL.final_price,
            TL.trade_timestamp,
            CASE
                WHEN TL.seller_id = ? THEN '판매자'
                ELSE '구매자'
            END AS my_role
        FROM Trade_Log TL
        JOIN User U
          ON U.user_id = CASE
                            WHEN TL.seller_id = ? THEN TL.buyer_id
                            ELSE TL.seller_id
                         END
        WHERE (TL.seller_id = ? OR TL.buyer_id = ?)
          AND NOT EXISTS (
              SELECT 1
              FROM Review R
              WHERE R.writer_id = ?
                AND R.mode = 'Trade'
                AND R.trade_id = TL.trade_id
          )
        ORDER BY TL.trade_timestamp DESC
    """, (user_id, user_id, user_id, user_id, user_id, user_id))

    rows = cur.fetchall()

    if not rows:
        print("📌 아직 후기를 쓸 거래가 없습니다.")
        con.close()
        return

    print("\n📋 후기 작성 가능한 거래 목록:")
    for r in rows:
        trade_id = r[0]
        target_username = r[2]
        price = r[3]
        ts = r[4]
        my_role = r[5]
        print(f"[{trade_id}] ({my_role}) 상대:{target_username} | 가격:{price} | 날짜:{ts}")

    select_id = input("\n후기를 작성할 trade_id 입력 (0=취소): ")
    if select_id == "0":
        con.close()
        return

    # 선택한 거래 찾기
    target = None
    for r in rows:
        if str(r[0]) == select_id:
            target = r
            break

    if not target:
        print("❌ 잘못된 trade_id")
        con.close()
        return

    trade_id = target[0]
    target_user_id = target[1]
    target_username = target[2]

    print(f"\n➡ {target_username} 님에 대한 거래 후기를 작성합니다.")

    rating_input = input("평가 (1=좋아요, 2=싫어요): ")

    if rating_input == "1":
        rating_int = 1
    elif rating_input == "2":
        rating_int = -1
    else:
        print("❌ 1 또는 2만 입력 가능합니다.")
        con.close()
        return

    content = input("후기 내용(엔터=생략 가능): ")

    # Review 테이블에 기록 (실제 컬럼 구조에 맞춤)
    cur.execute("""
        INSERT INTO Review
        (writer_id, target_user, trade_id, mode, rating, content)
        VALUES (?, ?, ?, 'Trade', ?, ?)
    """, (user_id, target_user_id, trade_id, rating_int, content))

    # User 평판 카운트 업데이트
    if rating_int == 1:
        cur.execute("""
            UPDATE User
            SET likes_count = likes_count + 1
            WHERE user_id = ?
        """, (target_user_id,))
    else:
        cur.execute("""
            UPDATE User
            SET dislikes_count = dislikes_count + 1
            WHERE user_id = ?
        """, (target_user_id,))

    con.commit()
    con.close()

    # 자동 등급 체크
    auto_role_check(target_user_id)

    print("✅ 거래 후기 등록 완료! (해당 거래는 다시 목록에 안 나옵니다.)")

# ================================
# 모임 후기 작성
# ================================


def write_event_review(user_id):
    con = sqlite3.connect("boardgame.db")
    cur = con.cursor()

    print("\n=== 🧑‍🤝‍🧑 모임 후기 작성 ===")

    # 내가 Approved로 참여한 모임 중, 아직 내가 리뷰 안 쓴 모임만
    cur.execute("""
        SELECT
            G.meeting_id,
            G.title,
            G.location,
            G.meet_date,
            G.host_id,
            U.username AS host_name
        FROM Gathering G
        JOIN Gathering_Participants GP
             ON G.meeting_id = GP.meeting_id
        JOIN User U
             ON G.host_id = U.user_id
        WHERE GP.user_id = ?
          AND GP.status = 'Approved'
          AND NOT EXISTS (
              SELECT 1
              FROM Review R
              WHERE R.writer_id = ?
                AND R.mode = 'Event'
                AND R.meeting_id = G.meeting_id
          )
        ORDER BY G.meet_date DESC
    """, (user_id, user_id))

    rows = cur.fetchall()

    if not rows:
        print("📌 아직 후기를 쓸 모임이 없습니다.")
        con.close()
        return

    print("\n📋 후기 작성 가능한 모임 목록:")
    for r in rows:
        meeting_id = r[0]
        title = r[1]
        loc = r[2]
        date = r[3]
        host_name = r[5]
        print(f"[{meeting_id}] {title} | 장소:{loc} | 날짜:{date} | 호스트:{host_name}")

    select_id = input("\n후기를 작성할 모임 ID 입력 (0=취소): ")
    if select_id == "0":
        con.close()
        return

    target = None
    for r in rows:
        if str(r[0]) == select_id:
            target = r
            break

    if not target:
        print("❌ 잘못된 모임 ID")
        con.close()
        return

    meeting_id = target[0]
    host_id = target[4]
    host_name = target[5]

    print(f"\n➡ 호스트 {host_name} 님에 대한 모임 후기를 작성합니다.")

    rating_input = input("평가 (1=좋아요, 2=싫어요): ")

    if rating_input == "1":
        rating_int = 1
    elif rating_input == "2":
        rating_int = -1
    else:
        print("❌ 1 또는 2만 입력 가능합니다.")
        con.close()
        return

    content = input("후기 내용(엔터=생략 가능): ")

    # Review 테이블에 기록
    cur.execute("""
        INSERT INTO Review
        (writer_id, target_user, meeting_id, mode, rating, content)
        VALUES (?, ?, ?, 'Event', ?, ?)
    """, (user_id, host_id, meeting_id, rating_int, content))

    # 호스트 평판 카운트 업데이트
    if rating_int == 1:
        cur.execute("""
            UPDATE User
            SET likes_count = likes_count + 1
            WHERE user_id = ?
        """, (host_id,))
    else:
        cur.execute("""
            UPDATE User
            SET dislikes_count = dislikes_count + 1
            WHERE user_id = ?
        """, (host_id,))

    con.commit()
    con.close()

    # 자동 등급 체크
    auto_role_check(host_id)

    print("✅ 모임 후기 등록 완료! (해당 모임은 다시 목록에 안 나옵니다.)")

# ================================
# 내 평판 보기
# ================================


def view_my_reputation(user_id):

    con = sqlite3.connect("boardgame.db")
    cur = con.cursor()

    cur.execute("""
        SELECT username, likes_count, dislikes_count, role
        FROM User
        WHERE user_id=?
    """, (user_id,))

    u = cur.fetchone()
    con.close()

    print("\n=== 😃 내 평판 ===")
    print(f"ID : {u[0]}")
    print(f"👍 좋아요 : {u[1]}")
    print(f"👎 싫어요 : {u[2]}")
    print(f"⭐ 등급 : {u[3]}")

# ================================
# 등급 신청
# ================================


def request_role_upgrade(user_id):
    con = sqlite3.connect("boardgame.db")
    cur = con.cursor()

    # 테이블이 없을 수도 있으니 안전하게 생성
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

    # 유저의 현재 등급 + 좋아요/싫어요 함께 가져오기
    cur.execute("""
        SELECT role, likes_count, dislikes_count
        FROM User
        WHERE user_id=?
    """, (user_id,))
    row = cur.fetchone()

    if not row:
        print("❌ 유저 정보 없음")
        con.close()
        return

    role, likes, dislikes = row
    score = likes - dislikes

    print("\n=== 등급 신청 ===")
    print(f"현재 등급 : {role}")
    print(f"👍 좋아요 : {likes}, 👎 싫어요 : {dislikes}, 평판 점수 : {score}")

    # -----------------------------
    # 1) User → VIP 신청 기준 체크
    # -----------------------------
    if role == "User":
        print("\n📌 VIP 승급 신청 기준")
        print("- 좋아요 10개 이상")
        print("- 싫어요 2개 이하")
        print("- 평판 점수(좋아요-싫어요) 8 이상\n")

        if likes < 10 or dislikes > 2 or score < 8:
            print("❌ 아직 VIP 신청 기준을 만족하지 못했습니다.")
            con.close()
            return

        print("✅ 기준을 만족합니다. VIP 승급 신청 가능!")
        target_role = "VIP"

    # -----------------------------
    # 2) BadUser → User 복구 신청 기준 체크
    # -----------------------------
    elif role == "BadUser":
        print("\n📌 일반 유저 복구 신청 기준")
        print("- 좋아요 3개 이상\n")

        if likes < 3:
            print("❌ 아직 일반 유저 복구 기준을 만족하지 못했습니다.")
            con.close()
            return

        print("✅ 기준을 만족합니다. 일반 유저 복구 신청 가능!")
        target_role = "User"

    else:
        print("현재 등급에서는 신청 가능한 등급이 없습니다.")
        con.close()
        return

    # 실제 신청 여부 확인
    choice = input("등급 신청을 진행할까요? (1=예, 0=취소): ")

    if choice != "1":
        print("신청이 취소되었습니다.")
        con.close()
        return

    # Role_Request 테이블에 신청 기록
    cur.execute("""
        INSERT INTO Role_Request (user_id, current_role, request_role)
        VALUES (?, ?, ?)
    """, (user_id, role, target_role))

    con.commit()
    con.close()

    print("✅ 등급 신청 완료! (관리자 승인 대기 중)")

# ================================
# 관리자 메뉴
# ================================


def admin_menu():
    con = sqlite3.connect("boardgame.db")
    cur = con.cursor()

    # 테이블이 없을 수도 있으니 안전하게 생성
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

    while True:
        print("\n=== ADMIN MENU ===")
        print("1. 등급 신청 목록")
        print("2. 승인 처리")
        print("3. 전체 유저 목록 보기")
        print("4. 모임 글 삭제")
        print("5. 판매글 삭제")
        print("0. 나가기")

        c = input("선택: ")

        if c == "1":
            cur.execute(
                "SELECT request_id, user_id, current_role, request_role, status, request_date FROM Role_Request WHERE status='Pending'")
            rows = cur.fetchall()
            if not rows:
                print("📌 대기 중인 신청 없음")
            else:
                for r in rows:
                    print(
                        f"[{r[0]}] user:{r[1]} | {r[2]} -> {r[3]} | 상태:{r[4]} | 신청일:{r[5]}")

        elif c == "2":
            rid = input("승인할 request_id 입력: ")

            cur.execute("""
                SELECT user_id, request_role
                FROM Role_Request
                WHERE request_id=? AND status='Pending'
            """, (rid,))
            row = cur.fetchone()

            if not row:
                print("❌ 잘못된 번호 또는 이미 처리됨")
                continue

            uid, target_role = row

            cur.execute("UPDATE User SET role=? WHERE user_id=?",
                        (target_role, uid))
            cur.execute(
                "UPDATE Role_Request SET status='Approved' WHERE request_id=?", (rid,))

            con.commit()
            print("✅ 승인 완료")
        elif c == "3":
            show_all_users()
        elif c == "4":
            delete_gathering_by_admin()
        elif c == "5":
            delete_listing_by_admin()

        elif c == "0":
            break
        else:
            print("❌ 잘못된 입력입니다.")

    con.close()


def show_all_users():
    con = sqlite3.connect("boardgame.db")
    cur = con.cursor()

    print("\n=== 👥 전체 유저 목록 (관리자 전용) ===")

    cur.execute("""
        SELECT
            user_id,
            username,
            likes_count,
            dislikes_count,
            (likes_count - dislikes_count) AS reputation_score,
            role
        FROM User
        ORDER BY reputation_score DESC
    """)

    rows = cur.fetchall()

    if not rows:
        print("❌ 등록된 유저가 없습니다.")
        con.close()
        return

    print("-" * 60)
    print("ID | Username | 👍 | 👎 | ⭐평판 | 등급")
    print("-" * 60)

    for r in rows:
        print(f"{r[0]:<3} | {r[1]:<10} | {r[2]:<3} | {r[3]:<3} | {r[4]:<5} | {r[5]}")

    print("-" * 60)
    con.close()


# ================================
# 자동 등급 체크
# ================================

def auto_role_check(target_user_id):
    con = sqlite3.connect("boardgame.db")
    cur = con.cursor()

    # 현재 유저의 좋아요/싫어요/등급 조회
    cur.execute("""
        SELECT likes_count, dislikes_count, role
        FROM User
        WHERE user_id=?
    """, (target_user_id,))

    row = cur.fetchone()
    if not row:
        con.close()
        return

    likes, dislikes, role = row
    score = likes - dislikes  # 평판 점수

    # 1) User → BadUser 자동 강등
    #    - 싫어요 5개 이상
    #    - 점수 <= 0 (싫어요가 같거나 더 많음)
    if role != "BadUser" and dislikes >= 5 and score <= 0:
        cur.execute("UPDATE User SET role='BadUser' WHERE user_id=?",
                    (target_user_id,))
        print("⚠️ 상대방이 BadUser 로 강등되었습니다 (싫어요 누적)")

    # 2) VIP → User 자동 강등
    #    - 좋아요가 8 미만이 되거나
    #    - 싫어요가 3개 이상이 되면
    elif role == "VIP" and (likes < 8 or dislikes >= 3):
        cur.execute("UPDATE User SET role='User' WHERE user_id=?",
                    (target_user_id,))
        print("⬇ VIP → 일반 유저로 강등되었습니다 (평판 하락)")

    con.commit()
    con.close()


def delete_gathering_by_admin():
    con = sqlite3.connect("boardgame.db")
    cur = con.cursor()

    print("\n=== 🗑 모임 글 삭제 ===")

    cur.execute("""
        SELECT meeting_id, title, meet_date, location
        FROM Gathering
        ORDER BY meet_date
    """)

    rows = cur.fetchall()

    if not rows:
        print("❌ 삭제할 모임이 없습니다.")
        con.close()
        return

    print("\n삭제 가능한 모임 목록:")
    for r in rows:
        print(f"[{r[0]}] {r[1]} | {r[3]} | {r[2]}")

    gid = input("\n삭제할 모임 ID (0=취소): ")

    if gid == "0":
        con.close()
        return

    # 참가 정보 먼저 삭제
    cur.execute("DELETE FROM Gathering_Participants WHERE meeting_id=?", (gid,))

    # 모임 삭제
    cur.execute("DELETE FROM Gathering WHERE meeting_id=?", (gid,))

    con.commit()
    con.close()

    print("✅ 모임 글 삭제 완료")


def delete_listing_by_admin():

    con = sqlite3.connect("boardgame.db")
    cur = con.cursor()

    print("\n=== 🗑 판매글 삭제 ===")

    cur.execute("""
        SELECT
            ML.listing_id,
            BM.title,
            ML.price,
            U.username,
            UC.collection_id
        FROM Market_Listing ML
        JOIN User_Collection UC ON ML.collection_id = UC.collection_id
        JOIN BoardGame_Master BM ON UC.game_id = BM.game_id
        JOIN User U ON ML.seller_id = U.user_id
    """)

    rows = cur.fetchall()

    if not rows:
        print("❌ 삭제할 판매글이 없습니다.")
        con.close()
        return

    print("\n삭제 가능한 판매 목록:")
    for r in rows:
        print(f"[{r[0]}] {r[1]} | {r[3]} | {r[2]}원")

    lid = input("\n삭제할 listing_id (0=취소): ")

    if lid == "0":
        con.close()
        return

    target = None
    for r in rows:
        if str(r[0]) == lid:
            target = r
            break

    if not target:
        print("❌ 잘못된 ID")
        con.close()
        return

    collection_id = target[4]

    # 보드게임 상태 복구
    cur.execute("""
        UPDATE User_Collection 
        SET status='Available'
        WHERE collection_id=?
    """, (collection_id,))

    # 판매글 삭제
    cur.execute("DELETE FROM Market_Listing WHERE listing_id=?", (lid,))

    con.commit()
    con.close()

    print("✅ 판매글 삭제 완료")


def complete_trade(user_id):
    con = sqlite3.connect("boardgame.db")
    cur = con.cursor()

    cur.execute("""
        SELECT listing_id, collection_id, buyer_id, price
        FROM Market_Listing
        WHERE seller_id=? AND status='Paid'
    """,(user_id,))

    rows = cur.fetchall()
    if not rows:
        print("📌 완료할 거래 없음")
        con.close()
        return

    for r in rows:
        print(f"[{r[0]}] 판매 완료 처리 가능")

    lid=input("완료할 거래 ID:")

    cur.execute("""
        SELECT collection_id,buyer_id,price
        FROM Market_Listing WHERE listing_id=?
    """,(lid,))

    cid,buyer,price = cur.fetchone()

    cur.execute("""
        INSERT INTO Trade_Log
        (listing_id,seller_id,buyer_id,final_price)
        VALUES (?,?,?,?)
    """,(lid,user_id,buyer,price))

    cur.execute("""
        UPDATE User_Collection
        SET owner_id=?, status='Sold'
        WHERE collection_id=?
    """,(buyer,cid))

    cur.execute("DELETE FROM Market_Listing WHERE listing_id=?", (lid,))

    con.commit()
    con.close()

    print("🎉 거래 종료 완료!")


# ================================
# 실행
# ================================
if __name__ == "__main__":
    start()
