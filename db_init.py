# db_init.py — Banco do TIÃO IA PURA (PostgreSQL no Render)

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os

# ----------------------------------------
# CONFIGURAÇÃO DO SERVIDOR (Render)
# ----------------------------------------
PG_HOST = "dpg-d4ob2p8gjchc73elo7k0-a.frankfurt-postgres.render.com"
PG_PORT = "5432"
PG_USER = "tiao_ia_db_user"
PG_PASS = os.getenv("PG_PASS")  # A senha será colocada no Render
PG_DB   = "tiao_ia_db"

def create_database():
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASS,
        sslmode="require"
    )

    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()

    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (PG_DB,))
    exists = cur.fetchone()

    if not exists:
        cur.execute(f"CREATE DATABASE {PG_DB}")
        print(f"[TIÃO] Banco '{PG_DB}' criado.")
    else:
        print(f"[TIÃO] Banco '{PG_DB}' já existe.")

    cur.close()
    conn.close()

def create_tables():
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASS,
        sslmode="require"
    )

    cur = conn.cursor()

    # Candles da Binance
    cur.execute("""
    CREATE TABLE IF NOT EXISTS candles (
        id SERIAL PRIMARY KEY,
        pair TEXT NOT NULL,
        timeframe TEXT NOT NULL,
        open_time BIGINT NOT NULL,
        open REAL NOT NULL,
        high REAL NOT NULL,
        low REAL NOT NULL,
        close REAL NOT NULL,
        volume REAL NOT NULL,
        UNIQUE(pair, timeframe, open_time)
    );
    """)

    # Features calculadas
    cur.execute("""
    CREATE TABLE IF NOT EXISTS features (
        id SERIAL PRIMARY KEY,
        pair TEXT NOT NULL,
        timeframe TEXT NOT NULL,
        open_time BIGINT NOT NULL,
        close REAL,
        rsi_14 REAL,
        ema_9 REAL,
        ema_21 REAL,
        ema_50 REAL,
        macd REAL,
        macd_signal REAL,
        macd_hist REAL,
        vol REAL,
        UNIQUE(pair, timeframe, open_time)
    );
    """)

    # Labels do aprendizado
    cur.execute("""
    CREATE TABLE IF NOT EXISTS labels (
        id SERIAL PRIMARY KEY,
        pair TEXT NOT NULL,
        timeframe TEXT NOT NULL,
        open_time BIGINT NOT NULL,
        target_pump INTEGER NOT NULL,
        UNIQUE(pair, timeframe, open_time)
    );
    """)

    # Previsões do TIÃO
    cur.execute("""
    CREATE TABLE IF NOT EXISTS predictions (
        id SERIAL PRIMARY KEY,
        pair TEXT NOT NULL,
        timeframe TEXT NOT NULL,
        open_time BIGINT NOT NULL,
        prob_pump REAL NOT NULL,
        model_version TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT NOW()
    );
    """)

    # Resultados reais (TIÃO se avalia)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS outcomes (
        id SERIAL PRIMARY KEY,
        prediction_id INTEGER REFERENCES predictions(id),
        actual_return REAL,
        pump_occurred INTEGER,
        evaluated_at TIMESTAMP DEFAULT NOW()
    );
    """)

    conn.commit()
    cur.close()
    conn.close()

    print("[TIÃO] Tabelas criadas com sucesso.")

# 🔥 CORREÇÃO AQUI (só isso)
if __name__ == "__main__":
    create_tables()


