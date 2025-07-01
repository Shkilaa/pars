# parser.py
# -*- coding: utf-8 -*-

from __future__ import annotations
from datetime import datetime
import json
import logging
import os
import random
import sqlite3
import time
from typing import Dict, List, Tuple

import requests

# -------------------------- НАСТРОЙКИ -------------------------------------
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
CHAT_IDS = [int(cid) for cid in os.getenv("CHAT_IDS", "").replace(" ", "").split(",") if cid]

if not TG_BOT_TOKEN or not CHAT_IDS:
    raise RuntimeError("TG_BOT_TOKEN или CHAT_IDS не заданы в переменных окружения")

MAX_PRICE = 50_000
ALLOWED_ROOMS = {1}

DB_FILE = "offers.db"
MSG_DELAY = 1.0                      # безопасная пауза между сообщениями

logging.basicConfig(format="%(asctime)s  %(levelname)s  %(message)s",
                    level=logging.INFO)

HEADERS = {"user-agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/118.0 Safari/537.36")}
TG_URL = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"

# -------------------------- БАЗА ДАННЫХ -----------------------------------
def db_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS offers(
            offer_id  INTEGER PRIMARY KEY,
            url       TEXT,
            price     INT,
            address   TEXT,
            area      REAL,
            rooms     INT,
            date      TEXT
        );
        CREATE TABLE IF NOT EXISTS sent(
            offer_id  INTEGER,
            chat_id   INTEGER,
            PRIMARY KEY (offer_id, chat_id)
        );
    """)
    return conn

# ----------------------- TELEGRAM-ОТПРАВКА --------------------------------
_last_sent: Dict[int, float] = {}      # chat_id → timestamp

def tg_send(chat_id: int, text: str) -> None:
    """Отправка сообщения в один чат с учётом лимитов и 429-retry."""
    pause = MSG_DELAY - (time.time() - _last_sent.get(chat_id, 0))
    if pause > 0:
        time.sleep(pause)

    while True:
        try:
            r = requests.post(
                TG_URL,
                data={
                    "chat_id": chat_id,
                    "text": text,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": False  # ссылка превью + картинка
                },
                timeout=10,
            )
            if r.status_code == 429:
                retry = r.json()["parameters"]["retry_after"]
                logging.warning("429 для %s, ждём %s c", chat_id, retry)
                time.sleep(retry)
                continue

            if not r.ok:
                logging.error("[TG %s] %s", chat_id, r.text)
            else:
                _last_sent[chat_id] = time.time()
                logging.info("Отправлено в %s", chat_id)
            break
        except Exception as exc:
            logging.error("[TG %s] %s", chat_id, exc)
            break

def broadcast(text: str) -> None:
    for cid in CHAT_IDS:
        tg_send(cid, text)

# -------------------------- ВСПОМОГАТЕЛЬНЫЕ --------------------------------
def accept(o: dict) -> bool:
    try:
        rooms = int(o["rooms"])
    except (TypeError, ValueError):
        return False
    return rooms in ALLOWED_ROOMS and o["price"] <= MAX_PRICE

def msg(o: dict) -> str:
    price = f"{o['price']:,}".replace(",", " ")
    # Ссылка первой строкой → Telegram сделает превью
    return (f"{o['url']}\n"
            f"<b>{price} ₽</b> · {o['rooms']}-к, {o['area']} м²\n"
            f"{o['address']}")

def process_offer(o: dict, conn: sqlite3.Connection) -> None:
    """Сохраняем объявление и рассылаем тем, кто ещё не получал."""
    if not accept(o):
        return
    cur = conn.cursor()
    cur.execute("""INSERT OR IGNORE INTO offers
                   VALUES (:offer_id,:url,:price,:address,:area,:rooms,:date)""", o)

    cur.execute("SELECT chat_id FROM sent WHERE offer_id=?", (o["offer_id"],))
    delivered = {row[0] for row in cur.fetchall()}
    to_send = [cid for cid in CHAT_IDS if cid not in delivered]
    if not to_send:
        return

    text = msg(o)
    for cid in to_send:
        tg_send(cid, text)
        cur.execute("INSERT OR IGNORE INTO sent VALUES (?,?)", (o["offer_id"], cid))
    conn.commit()

# ------------------------------- C I A N -----------------------------------
def cian_api() -> dict | None:
    query = {
        "jsonQuery": {
            "region": {"type": "terms", "value": [1]},
            "_type": "flatrent",
            "room": {"type": "terms", "value": [1]},
            "engine_version": {"type": "term", "value": 2},
            "for_day": {"type": "term", "value": "!1"},
            "is_by_homeowner": {"type": "term", "value": True},
            "sort": {"type": "term", "value": "creation_date_desc"},
            "bargain_terms": {"type": "range", "value": {"lte": MAX_PRICE}}
        }
    }
    try:
        r = requests.post("https://api.cian.ru/search-offers/v2/search-offers-desktop/",
                          headers=HEADERS,
                          data=json.dumps(query, ensure_ascii=False),
                          timeout=20)
        r.raise_for_status()
        return r.json()
    except Exception as exc:
        logging.error("[CIAN] %s", exc)
        return None

def cian_offer(it: dict) -> dict:
    return {
        "url":      it["fullUrl"],
        "offer_id": it["id"],
        "date":     datetime.fromtimestamp(it["addedTimestamp"]).strftime("%Y-%m-%d %H:%M:%S"),
        "price":    it["bargainTerms"]["priceRur"],
        "address":  it["geo"]["userInput"],
        "area":     it["totalArea"],
        "rooms":    it["roomsCount"],
    }

def parse_cian(conn: sqlite3.Connection) -> Tuple[int, int]:
    js = cian_api()
    if not js:
        return 0, 0
    processed = sent = 0
    for it in js["data"]["offersSerialized"]:
        processed += 1
        before = conn.total_changes
        process_offer(cian_offer(it), conn)
        sent += conn.total_changes - before
    logging.info("[CIAN] %s обработано, %s новых доставлено", processed, sent)
    return processed, sent

# --------------------------- YANDEX REALTY ---------------------------------
RETRIES, BACKOFF = 5, (1, 3)

def ya_api() -> dict | None:
    prov = ["search", "filters", "searchParams", "seo", "queryId",
            "forms", "filtersParams", "searchPresets", "react-search-data"]
    params = [("_providers", p) for p in prov] + [
        ("sort", "DATE_DESC"),
        ("rgid", "741964"),
        ("type", "RENT"),
        ("category", "APARTMENT"),
        ("agents", "NO"),
        ("_pageType", "search"),
        ("roomsTotalMin", "1"), ("roomsTotalMax", "1"),
        ("priceMax", str(MAX_PRICE)),
    ]
    for i in range(1, RETRIES + 1):
        try:
            r = requests.get("https://realty.yandex.ru/gate/react-page/get/",
                             headers=HEADERS, params=params, timeout=20)
            if 500 <= r.status_code < 600:
                raise requests.HTTPError(f"{r.status_code}", response=r)
            r.raise_for_status()
            return r.json()
        except Exception as exc:
            if i == RETRIES:
                logging.error("[YA] %s", exc)
                return None
            pause = random.uniform(*BACKOFF) * i
            logging.warning("[YA] попытка %s/%s — %s; ждём %.1f c",
                            i, RETRIES, exc, pause)
            time.sleep(pause)

def ya_offer(it: dict) -> dict:
    raw = it.get("updateDate") or it["creationDate"]
    return {
        "url":      it["shareUrl"],
        "offer_id": it["offerId"],
        "date":     raw.replace("T", " ").replace("Z", ""),
        "price":    it["price"]["value"],
        "address":  it["location"]["address"],
        "area":     it["area"]["value"],
        "rooms":    it["roomsTotalKey"],
    }

def parse_yandex(conn: sqlite3.Connection) -> Tuple[int, int]:
    js = ya_api()
    if not js:
        return 0, 0
    processed = sent = 0
    for it in js["response"]["search"]["offers"]["entities"]:
        processed += 1
        before = conn.total_changes
        process_offer(ya_offer(it), conn)
        sent += conn.total_changes - before
    logging.info("[YA] %s обработано, %s новых доставлено", processed, sent)
    return processed, sent

# -------------------------------- MAIN ------------------------------------
def main() -> None:
    with db_conn() as conn:
        c_tot, c_new = parse_cian(conn)
        y_tot, y_new = parse_yandex(conn)

    if c_new > 0 or y_new > 0:
        broadcast(
            f"ℹ️ <b>Сводка</b>\n"
            f"Циан   — {c_tot} / новых {c_new}\n"
            f"Яндекс — {y_tot} / новых {y_new}"
        )

if __name__ == "__main__":
    main()
