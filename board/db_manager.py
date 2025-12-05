import sqlite3
import pandas as pd

class BoardLinkDB:
    def __init__(self, db_path="boardgame.db"):
        self.db_path = db_path

    def get_connection(self):
        return sqlite3.connect(self.db_path)

    def run_query(self, query, params=()):
        conn = self.get_connection()
        try:
            return pd.read_sql(query, conn, params=params)
        finally:
            conn.close()

    def execute_query(self, query, params=()):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(query, params)
            conn.commit()
            return cursor.rowcount
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()

    # ==========================
    # 1. 인증 (Auth)
    # ==========================
    def login(self, username, password):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT user_id, role, username FROM User WHERE username=? AND password_hash=?", (username, password))
        row = cursor.fetchone()
        conn.close()
        return row

    def sign_up(self, username, password, location):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM User WHERE username=?", (username,))
        if cursor.fetchone():
            conn.close()
            return False, "이미 존재하는 ID입니다."
        
        try:
            cursor.execute("INSERT INTO User (username, password_hash, location_info, role) VALUES (?, ?, ?, 'User')", (username, password, location))
            conn.commit()
            return True, "회원가입 완료!"
        except Exception as e:
            return False, str(e)
        finally:
            conn.close()

    def get_user_info(self, user_id):
        return self.run_query("SELECT * FROM User WHERE user_id = ?", (user_id,))

    # ==========================
    # 2. 보드게임 (Game & Collection)
    # ==========================
    def get_my_collection(self, user_id):
        query = """
            SELECT UC.collection_id, BM.title, BM.genre, UC.condition_rank, UC.status
            FROM User_Collection UC
            JOIN BoardGame_Master BM ON UC.game_id = BM.game_id
            WHERE UC.owner_id = ?
        """
        return self.run_query(query, (user_id,))

    def register_game_to_collection(self, user_id, title, condition, genre, min_p, max_p, time, diff):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            # 게임 마스터 확인/등록
            cursor.execute("SELECT game_id FROM BoardGame_Master WHERE title=?", (title,))
            row = cursor.fetchone()
            if row:
                game_id = row[0]
            else:
                cursor.execute("""
                    INSERT INTO BoardGame_Master (title, genre, min_players, max_players, avg_playtime, difficulty)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (title, genre, min_p, max_p, time, diff))
                game_id = cursor.lastrowid

            # 컬렉션 등록
            cursor.execute("INSERT INTO User_Collection (owner_id, game_id, condition_rank) VALUES (?, ?, ?)", (user_id, game_id, condition))
            conn.commit()
            return True, "등록 완료"
        except Exception as e:
            conn.rollback()
            return False, str(e)
        finally:
            conn.close()

    # ==========================
    # 3. 모임 (Gathering)
    # ==========================
    def search_gatherings(self, loc=None):
        # description 컬럼도 조회하도록 추가
        query = """
            SELECT G.meeting_id, G.title, G.description, G.location, G.meet_date, 
                   G.current_participants, G.max_participants, G.status, U.username as host_name
            FROM Gathering G
            JOIN User U ON G.host_id = U.user_id
        """
        params = []
        if loc:
            query += " WHERE G.location LIKE ?"
            params.append(f'%{loc}%')
        
        query += " ORDER BY CASE WHEN G.status='Open' THEN 1 ELSE 2 END, G.meet_date ASC"
        return self.run_query(query, params)

    # [수정됨] description 파라미터 추가
    def create_gathering(self, user_id, title, desc, loc, date_str, max_p):
        try:
            self.execute_query("""
                INSERT INTO Gathering (host_id, title, description, location, meet_date, max_participants, current_participants, status)
                VALUES (?, ?, ?, ?, ?, ?, 0, 'Open')
            """, (user_id, title, desc, loc, date_str, max_p))
            return True, "모임 개설 완료"
        except Exception as e:
            return False, str(e)

    def join_gathering(self, user_id, meeting_id):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT status FROM Gathering WHERE meeting_id=?", (meeting_id,))
            row = cursor.fetchone()
            if not row: return False, "모임이 없습니다."
            if row[0] != 'Open': return False, "이미 종료된 모임입니다."

            cursor.execute("SELECT role FROM User WHERE user_id=?", (user_id,))
            role = cursor.fetchone()[0]

            cursor.execute("SELECT 1 FROM Gathering_Participants WHERE meeting_id=? AND user_id=?", (meeting_id, user_id))
            if cursor.fetchone():
                return False, "이미 신청했습니다."

            cursor.execute("SELECT COALESCE(MAX(wait_order), 0) FROM Gathering_Participants WHERE meeting_id=? AND status='Waitlist'", (meeting_id,))
            max_order = cursor.fetchone()[0]

            if role == "BadUser":
                my_order = max_order + 1
                msg = "BadUser: 대기열 최하위 배정"
            elif role == "VIP":
                cursor.execute("UPDATE Gathering_Participants SET wait_order = wait_order + 1 WHERE meeting_id=? AND status='Waitlist'", (meeting_id,))
                my_order = 1
                msg = "VIP: 대기열 1순위 배정"
            else:
                my_order = max_order + 1
                msg = f"대기열 {my_order}번 배정"

            cursor.execute("INSERT INTO Gathering_Participants (meeting_id, user_id, status, wait_order) VALUES (?, ?, 'Waitlist', ?)", (meeting_id, user_id, my_order))
            conn.commit()
            return True, msg
        except Exception as e:
            conn.rollback()
            return False, str(e)
        finally:
            conn.close()

    def get_my_hosted_gatherings(self, host_id):
        return self.run_query("SELECT * FROM Gathering WHERE host_id=? ORDER BY meet_date DESC", (host_id,))

    def get_gathering_applicants(self, meeting_id):
        query = """
            SELECT GP.user_id, U.username, U.role, U.likes_count, U.dislikes_count, GP.status, GP.wait_order
            FROM Gathering_Participants GP
            JOIN User U ON GP.user_id = U.user_id
            WHERE GP.meeting_id = ? AND GP.status = 'Waitlist'
            ORDER BY GP.wait_order ASC
        """
        return self.run_query(query, (meeting_id,))
    
    def approve_gathering_participant(self, meeting_id, target_user_id):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT max_participants, current_participants FROM Gathering WHERE meeting_id=?", (meeting_id,))
            mx, cur = cursor.fetchone()
            if cur >= mx:
                return False, "정원 초과"

            cursor.execute("UPDATE Gathering_Participants SET status='Approved' WHERE meeting_id=? AND user_id=?", (meeting_id, target_user_id))
            if cursor.rowcount > 0:
                cursor.execute("UPDATE Gathering SET current_participants = current_participants + 1 WHERE meeting_id=?", (meeting_id,))
                conn.commit()
                return True, "승인 완료"
            return False, "대상자를 찾을 수 없음"
        finally:
            conn.close()

    def reject_gathering_participant(self, meeting_id, target_user_id):
        return self.execute_query("UPDATE Gathering_Participants SET status='Rejected' WHERE meeting_id=? AND user_id=?", (meeting_id, target_user_id))

    def close_gathering(self, meeting_id):
        return self.execute_query("UPDATE Gathering SET status='Closed' WHERE meeting_id=?", (meeting_id,))

    def get_my_applications(self, user_id):
        query = """
            SELECT G.title, G.meet_date, G.location, GP.status, GP.wait_order
            FROM Gathering_Participants GP
            JOIN Gathering G ON GP.meeting_id = G.meeting_id
            WHERE GP.user_id = ?
            ORDER BY G.meet_date DESC
        """
        return self.run_query(query, (user_id,))

    # ==========================
    # 4. 중고 거래 (Market)
    # ==========================
    def register_market(self, user_id, col_id, price, desc):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT role FROM User WHERE user_id=?", (user_id,))
            if cursor.fetchone()[0] == "BadUser": return False, "BadUser는 판매 불가"
            cursor.execute("INSERT INTO Market_Listing (collection_id, seller_id, price, description) VALUES (?, ?, ?, ?)", (col_id, user_id, price, desc))
            cursor.execute("UPDATE User_Collection SET status='In_Trade' WHERE collection_id=?", (col_id,))
            conn.commit()
            return True, "판매 등록 완료"
        except Exception as e:
            conn.rollback()
            return False, str(e)
        finally:
            conn.close()

    def get_market_list(self):
        query = """
            SELECT ML.listing_id, BM.title, ML.price, U.username, U.role, ML.status
            FROM Market_Listing ML
            JOIN User_Collection UC ON ML.collection_id = UC.collection_id
            JOIN BoardGame_Master BM ON UC.game_id = BM.game_id
            JOIN User U ON ML.seller_id = U.user_id
            WHERE ML.buyer_id IS NULL AND UC.status='In_Trade'
            ORDER BY CASE WHEN U.role='VIP' THEN 1 ELSE 2 END, ML.listing_id DESC
        """
        return self.run_query(query)

    def request_purchase(self, user_id, listing_id):
        return self.execute_query("UPDATE Market_Listing SET buyer_id=?, status='Requested' WHERE listing_id=?", (user_id, listing_id))

    def approve_trade_request(self, listing_id):
        return self.execute_query("UPDATE Market_Listing SET status='Approved' WHERE listing_id=?", (listing_id,))

    def get_ongoing_trades(self, user_id):
        query = """
            SELECT listing_id, status, seller_id, buyer_id, seller_account, buyer_address
            FROM Market_Listing
            WHERE (seller_id=? OR buyer_id=?) AND status IN ('Approved', 'Paid')
        """
        return self.run_query(query, (user_id, user_id))

    def update_trade_info(self, listing_id, user_id, info_type, value):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            col = "seller_account" if info_type == 'account' else "buyer_address"
            cursor.execute(f"UPDATE Market_Listing SET {col}=? WHERE listing_id=?", (value, listing_id))
            cursor.execute("SELECT seller_account, buyer_address FROM Market_Listing WHERE listing_id=?", (listing_id,))
            acc, addr = cursor.fetchone()
            msg = "정보 입력 완료"
            if acc and addr:
                cursor.execute("UPDATE Market_Listing SET status='Paid' WHERE listing_id=?", (listing_id,))
                msg += " -> 양측 입력 확인됨! 입금(Paid) 상태로 전환."
            conn.commit()
            return True, msg
        finally:
            conn.close()

    def complete_trade_transaction(self, listing_id, seller_id):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT collection_id, buyer_id, price FROM Market_Listing WHERE listing_id=? AND status='Paid'", (listing_id,))
            row = cursor.fetchone()
            if not row: return False, "완료 가능한 거래가 아닙니다."
            cid, buyer, price = row
            cursor.execute("INSERT INTO Trade_Log (listing_id, seller_id, buyer_id, final_price) VALUES (?, ?, ?, ?)", (listing_id, seller_id, buyer, price))
            cursor.execute("UPDATE User_Collection SET owner_id=?, status='Sold' WHERE collection_id=?", (buyer, cid))
            cursor.execute("DELETE FROM Market_Listing WHERE listing_id=?", (listing_id,))
            conn.commit()
            return True, "거래가 최종 완료되었습니다!"
        except Exception as e:
            conn.rollback()
            return False, str(e)
        finally:
            conn.close()

    # ==========================
    # 5. 리뷰 & 평판
    # ==========================
    def _check_auto_downgrade(self, cursor, user_id):
        cursor.execute("SELECT likes_count, dislikes_count, role FROM User WHERE user_id=?", (user_id,))
        likes, dislikes, role = cursor.fetchone()
        new_role = role
        if role != "BadUser" and dislikes >= 5 and (likes - dislikes) <= 0: new_role = "BadUser"
        elif role == "VIP" and (likes < 8 or dislikes >= 3): new_role = "User"
        if new_role != role: cursor.execute("UPDATE User SET role=? WHERE user_id=?", (new_role, user_id))

    # ==========================
    # 6. 관리자
    # ==========================
    def get_all_users(self): return self.run_query("SELECT * FROM User")
    def delete_gathering_admin(self, meeting_id):
        conn = self.get_connection()
        try:
            conn.execute("DELETE FROM Gathering_Participants WHERE meeting_id=?", (meeting_id,))
            conn.execute("DELETE FROM Gathering WHERE meeting_id=?", (meeting_id,))
            conn.commit()
        finally: conn.close()
    def delete_listing_admin(self, listing_id):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT collection_id FROM Market_Listing WHERE listing_id=?", (listing_id,))
            row = cursor.fetchone()
            if row: cursor.execute("UPDATE User_Collection SET status='Available' WHERE collection_id=?", (row[0],))
            cursor.execute("DELETE FROM Market_Listing WHERE listing_id=?", (listing_id,))
            conn.commit()
        finally: conn.close()