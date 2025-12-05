import sqlite3

con = sqlite3.connect("boardgame.db")
cur = con.cursor()

# ---------- User Table ----------
cur.execute("""
CREATE TABLE IF NOT EXISTS User (
    user_id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    location_info TEXT,
    role TEXT DEFAULT 'User',
    likes_count INTEGER DEFAULT 0,
    dislikes_count INTEGER DEFAULT 0,
    join_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
""")

# ---------- BoardGame_Master ----------
cur.execute("""
CREATE TABLE IF NOT EXISTS BoardGame_Master (
    game_id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    genre TEXT,
    min_players INTEGER,
    max_players INTEGER,
    avg_playtime INTEGER,
    difficulty REAL
);
""")

# ---------- User_Collection ----------
cur.execute("""
CREATE TABLE IF NOT EXISTS User_Collection (
    collection_id INTEGER PRIMARY KEY AUTOINCREMENT,
    owner_id INTEGER NOT NULL,
    game_id INTEGER NOT NULL,
    purpose TEXT DEFAULT 'Event',
    status TEXT DEFAULT 'Available',
    condition_rank TEXT DEFAULT 'A'
);
""")

# ---------- Market_Listing ----------
cur.execute("""
CREATE TABLE IF NOT EXISTS Market_Listing (
    listing_id INTEGER PRIMARY KEY AUTOINCREMENT,
    collection_id INTEGER NOT NULL UNIQUE,
    seller_id INTEGER NOT NULL,
    price REAL NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
""")

# ---------- Trade_Log ----------
cur.execute("""
CREATE TABLE IF NOT EXISTS Trade_Log (
    trade_id INTEGER PRIMARY KEY AUTOINCREMENT,
    listing_id INTEGER NOT NULL,
    seller_id INTEGER NOT NULL,
    buyer_id INTEGER NOT NULL,
    final_price REAL,
    trade_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
""")

# ---------- Gathering ----------
cur.execute("""
CREATE TABLE IF NOT EXISTS Gathering (
    meeting_id INTEGER PRIMARY KEY AUTOINCREMENT,
    host_id INTEGER NOT NULL,
    title TEXT NOT NULL,
    location TEXT,
    meet_date TIMESTAMP,
    max_participants INTEGER,
    current_participants INTEGER DEFAULT 0,
    description TEXT,
    status TEXT DEFAULT 'Open'
);
""")

# ---------- Gathering_Games ----------
cur.execute("""
CREATE TABLE IF NOT EXISTS Gathering_Games (
    meeting_id INTEGER,
    game_id INTEGER,
    PRIMARY KEY (meeting_id, game_id)
);
""")

# ---------- Gathering_Participants ----------
cur.execute("""
CREATE TABLE IF NOT EXISTS Gathering_Participants (
    meeting_id INTEGER,
    user_id INTEGER,
    status TEXT DEFAULT 'Pending',
    request_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (meeting_id, user_id)
);
""")

# ---------- Review ----------
cur.execute("""
CREATE TABLE IF NOT EXISTS Review (
    review_id INTEGER PRIMARY KEY AUTOINCREMENT,
    writer_id INTEGER NOT NULL,
    target_id INTEGER NOT NULL,
    review_type TEXT NOT NULL,
    related_id INTEGER NOT NULL,
    rating_type TEXT NOT NULL,
    comment TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
""")

# ---------- Game_Review ----------
cur.execute("""
CREATE TABLE IF NOT EXISTS Game_Review (
    game_review_id INTEGER PRIMARY KEY AUTOINCREMENT,
    meeting_id INTEGER NOT NULL,
    game_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    score REAL NOT NULL
);
""")

con.commit()
con.close()

print("DB created!")