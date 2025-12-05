import streamlit as st
import pandas as pd
from db_manager import BoardLinkDB

db = BoardLinkDB("boardgame.db")

if "logged_in" not in st.session_state:
    st.session_state.logged_in = False
    st.session_state.user_id = None
    st.session_state.role = None
    st.session_state.username = None

# ==========================
# 로그인 / 회원가입 화면
# ==========================
def login_page():
    st.title("🎲 BoardLink (v2.2)")
    tab1, tab2 = st.tabs(["로그인", "회원가입"])

    with tab1:
        id_ = st.text_input("아이디")
        pw_ = st.text_input("비밀번호", type="password")
        if st.button("로그인"):
            res = db.login(id_, pw_)
            if res:
                st.session_state.logged_in = True
                st.session_state.user_id = res[0]
                st.session_state.role = res[1]
                st.session_state.username = res[2]
                st.rerun()
            else:
                st.error("실패")

    with tab2:
        nid = st.text_input("새 아이디")
        npw = st.text_input("새 비번", type="password")
        nloc = st.text_input("지역")
        if st.button("가입"):
            suc, msg = db.sign_up(nid, npw, nloc)
            if suc: st.success(msg)
            else: st.error(msg)

# ==========================
# 메인 앱
# ==========================
def main_app():
    st.sidebar.title(f"{st.session_state.username} ({st.session_state.role})")
    
    menus = ["홈", "보드게임", "모임", "중고장터", "마이페이지"]
    if st.session_state.role == "Admin":
        menus.append("관리자(Admin)")
        
    menu = st.sidebar.radio("이동", menus)
    
    if st.sidebar.button("로그아웃"):
        st.session_state.logged_in = False
        st.rerun()

    if menu == "홈":
        st.title("🏠 BoardLink Home")
        st.info("모임 개설 시 상세 설명을 추가할 수 있습니다.")

    elif menu == "보드게임":
        page_boardgame()

    elif menu == "모임":
        page_gathering()

    elif menu == "중고장터":
        page_market()
    
    elif menu == "마이페이지":
        page_mypage()

    elif menu == "관리자(Admin)":
        page_admin()

# --------------------------
# 페이지: 보드게임
# --------------------------
def page_boardgame():
    st.header("🧩 보드게임 관리")
    tab1, tab2 = st.tabs(["내 컬렉션", "게임 등록"])
    with tab1:
        df = db.get_my_collection(st.session_state.user_id)
        st.dataframe(df)
    with tab2:
        with st.form("reg_g"):
            title = st.text_input("게임명")
            cond = st.selectbox("상태", ["A","B","C"])
            if st.form_submit_button("등록"):
                db.register_game_to_collection(st.session_state.user_id, title, cond, "Etc", 2, 4, 30, 2.5)
                st.success("등록됨")

# --------------------------
# 페이지: 모임
# --------------------------
def page_gathering():
    st.header("🗓 모임 관리")
    tab1, tab2, tab3, tab4 = st.tabs(["모임 찾기", "내 신청 현황", "모임 개설", "호스트 관리"])

    with tab1:
        st.subheader("참여 가능한 모임")
        df = db.search_gatherings()
        
        if not df.empty:
            # 설명(description) 컬럼도 보여줍니다.
            st.dataframe(df.style.map(lambda x: 'color: green' if x == 'Open' else 'color: red', subset=['status']))
        
        mid = st.number_input("참여할 모임 ID", min_value=0)
        if st.button("참여 신청"):
            suc, msg = db.join_gathering(st.session_state.user_id, mid)
            if suc: st.success(msg)
            else: st.error(msg)

    with tab2:
        st.subheader("📋 내가 신청한 모임")
        my_apps = db.get_my_applications(st.session_state.user_id)
        
        if my_apps.empty:
            st.info("신청한 모임이 없습니다.")
        else:
            for idx, row in my_apps.iterrows():
                title = row['title']
                status = row['status']
                order = row['wait_order']
                
                if status == 'Approved':
                    st.success(f"✅ [참가 확정] {title} (날짜: {row['meet_date']})")
                elif status == 'Rejected':
                    st.error(f"❌ [거절됨] {title}")
                elif status == 'Waitlist':
                    st.warning(f"⏳ [대기중] {title} - 대기 순번: {order}번")
                else:
                    st.info(f"{title}: {status}")

    with tab3:
        st.subheader("새 모임 만들기")
        # [수정됨] 설명 입력창 추가
        title = st.text_input("제목")
        desc = st.text_input("한줄 설명", placeholder="어떤 모임인지 간단히 설명해주세요.")
        loc = st.text_input("장소")
        date = st.text_input("일시 (YYYY-MM-DD HH:MM)")
        mp = st.number_input("인원", value=4)
        
        if st.button("개설"):
            db.create_gathering(st.session_state.user_id, title, desc, loc, date, mp)
            st.success("개설됨")

    with tab4:
        st.subheader("👑 내가 주최한 모임 관리")
        hosted = db.get_my_hosted_gatherings(st.session_state.user_id)
        
        if hosted.empty:
            st.info("주최한 모임이 없습니다.")
        else:
            for idx, row in hosted.iterrows():
                mid = row['meeting_id']
                status = row['status']
                
                status_icon = "🟢 모집중" if status == 'Open' else "🔴 모임 종료"
                # Expandable 제목에 설명을 포함시켜 줍니다.
                with st.expander(f"[{status_icon}] {row['title']} - {row.get('description', '')}"):
                    
                    c1, c2 = st.columns([3, 1])
                    with c1:
                        st.write(f"장소: {row['location']} | 일시: {row['meet_date']}")
                        st.write(f"인원: {row['current_participants']}/{row['max_participants']}")
                    with c2:
                        if status == 'Open':
                            if st.button("모임 종료하기", key=f"close_{mid}"):
                                db.close_gathering(mid)
                                st.rerun()
                        else:
                            st.write("🏁 완료된 모임")

                    if status == 'Open':
                        st.divider()
                        st.markdown("#### 🙋 신청자 관리")
                        applicants = db.get_gathering_applicants(mid)
                        
                        if applicants.empty:
                            st.text("대기 중인 신청자가 없습니다.")
                        else:
                            for a_idx, app in applicants.iterrows():
                                uid = app['user_id']
                                uname = app['username']
                                role = app['role']
                                likes = app['likes_count']
                                dislikes = app['dislikes_count']
                                
                                role_badge = "👤"
                                if role == 'VIP': role_badge = "⭐ VIP"
                                elif role == 'BadUser': role_badge = "🚫 BadUser"
                                
                                col_info, col_btn1, col_btn2 = st.columns([4, 1, 1])
                                with col_info:
                                    st.write(f"**{uname}** ({role_badge}) | 👍 {likes} / 👎 {dislikes}")
                                with col_btn1:
                                    if st.button("승인", key=f"acc_{mid}_{uid}"):
                                        res, msg = db.approve_gathering_participant(mid, uid)
                                        if res: st.success(msg); st.rerun()
                                        else: st.error(msg)
                                with col_btn2:
                                    if st.button("거절", key=f"rej_{mid}_{uid}"):
                                        db.reject_gathering_participant(mid, uid)
                                        st.warning("거절됨"); st.rerun()

# --------------------------
# 페이지: 중고장터
# --------------------------
def page_market():
    st.header("🛒 중고장터")
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["목록/구매", "판매등록", "판매승인", "정보교환", "거래확정"])

    with tab1:
        df = db.get_market_list()
        st.dataframe(df)
        bid = st.number_input("구매할 ID", min_value=0)
        if st.button("구매 신청"):
            db.request_purchase(st.session_state.user_id, bid)
            st.success("신청 완료")

    with tab2:
        my_g = db.get_my_collection(st.session_state.user_id)
        st.dataframe(my_g)
        cid = st.number_input("판매할 Collection ID", min_value=0)
        price = st.number_input("가격", min_value=0)
        if st.button("판매 등록"):
            res, msg = db.register_market(st.session_state.user_id, cid, price, "설명")
            if res: st.success(msg)
            else: st.error(msg)

    with tab3:
        q = "SELECT listing_id, price, buyer_id FROM Market_Listing WHERE seller_id=? AND status='Requested'"
        reqs = db.run_query(q, (st.session_state.user_id,))
        st.dataframe(reqs)
        app_id = st.number_input("승인할 Listing ID", min_value=0, key="app_id")
        if st.button("구매 승인"):
            db.approve_trade_request(app_id)
            st.success("승인 완료")

    with tab4:
        ongoing = db.get_ongoing_trades(st.session_state.user_id)
        if not ongoing.empty:
            st.dataframe(ongoing)
            sel_id = st.number_input("정보 입력할 Listing ID", min_value=0, key="info_id")
            val = st.text_input("계좌번호/주소")
            if st.button("정보 입력"):
                row = ongoing[ongoing['listing_id'] == sel_id]
                if not row.empty:
                    type_ = 'account' if row.iloc[0]['seller_id'] == st.session_state.user_id else 'address'
                    suc, msg = db.update_trade_info(sel_id, st.session_state.user_id, type_, val)
                    st.info(msg)

    with tab5:
        q = "SELECT listing_id, price, buyer_id FROM Market_Listing WHERE seller_id=? AND status='Paid'"
        paid = db.run_query(q, (st.session_state.user_id,))
        st.dataframe(paid)
        fin_id = st.number_input("확정할 Listing ID", min_value=0, key="fin_id")
        if st.button("최종 완료"):
            suc, msg = db.complete_trade_transaction(fin_id, st.session_state.user_id)
            if suc: st.success(msg)

# --------------------------
# 페이지: 관리자
# --------------------------
def page_admin():
    st.header("👮 관리자 페이지")
    st.dataframe(db.get_all_users())
    del_mid = st.number_input("삭제할 모임 ID", min_value=0)
    if st.button("모임 삭제"): db.delete_gathering_admin(del_mid)
    del_lid = st.number_input("삭제할 판매 ID", min_value=0)
    if st.button("판매 삭제"): db.delete_listing_admin(del_lid)

def page_mypage():
    st.header("내 정보")
    df = db.get_user_info(st.session_state.user_id)
    st.dataframe(df)

if st.session_state.logged_in:
    main_app()
else:
    login_page()