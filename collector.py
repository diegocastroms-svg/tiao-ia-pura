# collector.py — Coletor de candles para o TIÃO IA PURA

import os
import time
import json
import urllib.request
import psycopg2

# ----------------------------------------
# CONFIGURAÇÃO DO SERVIDOR (Render / Postgres)
# ----------------------------------------
PG_HOST = "dpg-d4ob2p8gjchc73elo7k0-a.frankfurt-postgres.render.com"
PG_PORT = "5432"
PG_USER = "tiao_ia_db_user"
PG_PASS = os.getenv("PG_PASS")
PG_DB   = "tiao_ia_db"

# ----------------------------------------
# CONFIGURAÇÃO DO BINANCE E DO COLETOR
# ----------------------------------------
BINANCE_URL = "https://api.binance.com/api/v3/klines"

PAIRS = os.getenv("PAIRS", "BTCUSDT,ETHUSDT,SOLUSDT").split(",")
TIMEFRAME = os.getenv("TIMEFRAME", "15m")
LIMIT = int(os.getenv("LIMIT", "500"))


def get_connection():
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASS,
        dbname=PG_DB,
        sslmode="require"
    )
    return conn


def fetch_klines(pair: str, interval: str, limit: int = 500):
    params = f"?symbol={pair}&interval={interval}&limit={limit}"
    url = BINANCE_URL + params

    with urllib.request.urlopen(url) as resp:
        data = resp.read()
        klines = json.loads(data.decode("utf-8"))
        return klines


def save_candles(pair: str, timeframe: str, klines):
    conn = get_connection()
    cur = conn.cursor()

    inserted = 0

    for k in klines:
        open_time = int(k[0])
        _open = float(k[1])
        high = float(k[2])
        low = float(k[3])
        close = float(k[4])
        volume = float(k[5])

        cur.execute(
            """
            INSERT INTO candles (pair, timeframe, open_time, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (pair, timeframe, open_time) DO NOTHING;
            """,
            (pair, timeframe, open_time, _open, high, low, close, volume)
        )
        inserted += cur.rowcount

    conn.commit()
    cur.close()
    conn.close()

    print(f"[TIÃO][{pair}][{timeframe}] Candles novos inseridos: {inserted}")


def collect_once():
    print("[TIÃO] Iniciando coleta única de candles...")
    for p in PAIRS:
        pair = p.strip().upper()
        if not pair:
            continue
        try:
            print(f"[TIÃO] Buscando candles de {pair} ({TIMEFRAME})...")
            klines = fetch_klines(pair, TIMEFRAME, LIMIT)
            save_candles(pair, TIMEFRAME, klines)
        except Exception as e:
            print(f"[TIÃO] Erro ao coletar {pair}: {e}")
    print("[TIÃO] Coleta única finalizada.")


def collect_loop():
    interval_seconds = int(os.getenv("COLLECT_INTERVAL", "300"))
    print(f"[TIÃO] Iniciando loop contínuo (intervalo {interval_seconds}s)...")

    while True:
        collect_once()
        print(f"[TIÃO] Aguardando {interval_seconds} segundos para próxima rodada...")
        time.sleep(interval_seconds)


if __name__ == "__main__":
    collect_once()
