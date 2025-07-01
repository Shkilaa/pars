# parser.py
# -*- coding: utf-8 -*-
"""
Парсер 1-комнатных квартир (≤ 50 000 ₽) с Циана и Яндекс.Недвижимости
и рассылка объявлений в Telegram-чаты.  
• каждое объявление отправляется в каждый чат ровно ОДИН раз;  
• ссылка стоит первой строкой, поэтому Telegram показывает карточку-превью;  
• итоговая «сводка» отменена — бот шлёт только сами квартиры.
"""

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

# ────────────────────────────  ПАРАМЕТРЫ  ────────────────────────────────
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
CHAT_IDS = [int(i) for i in os.getenv("CHAT_IDS", "").replace(" ", "").split(",") if i]

if not TG_BOT_TOKEN or not CHAT_IDS:
    raise RuntimeError("TG_BOT_TOKEN или CHAT_IDS не заданы в переменных окружения")

MAX_PRICE     = 50_000
ALLOWED_ROOMS = {1}

DB_FILE   = "offers.db"
MSG_DELAY = 1.0                      # сек между личными сообщениями

logging.basicConfig(format="%(asctime)s  %(levelname)s  %(message)s",
                    level=logging.INFO)

HEADERS = {
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/118.0 Safari/537.36"
    )
}
TG_URL = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"

# ─────────────────────────────  БАЗА  ────────────────────────────────────
def db_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS offers(
            offer_id INTEGER PRIMARY KEY,
            url      TEXT,
            price    INT,
            address  TEXT,
            area     REAL,
            rooms    INT,
            date     TEXT
        );
        CREATE TABLE IF NOT EXISTS sent(
            offer_id INTEGER,
            chat_id  INTEGER,
            PRIMARY KEY (offer_id, chat_id)
        );
    """)
    return conn

# ───────────────────────  ОТПРАВКА В TELEGRAM  ──────────────────────────
_last: Dict[int, float] = {}

def tg_send(chat: int, text: str) -> None:
    """Отправляет сообщение в chat с учётом лимитов Telegram."""
    wait = MSG_DELAY - (time.time() - _last.get(chat, 0))
    if wait > 0:
        time.sleep(wait)

    while True:
        try:
            r = requests.post(
                TG_URL,
                data={
                    "chat_id": chat,
                    "text": text,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": False   # показываем превью-карточку
                },
                timeout=10,
            )

            if r.status_code == 429:
                retry = r.json()["parameters"]["retry_after"]
                logging.warning("429 для %s, пауза %s c", chat, retry)
                time.sleep(retry)
                continue

            if r.ok:
                _last[chat] = time.time()
                logging.info("Отправлено в чат %s", chat)
            else:
                logging.error("[TG %s] %s", chat, r.text)
            break
        except Exception as exc:
            logging.error("[TG %s] %s", chat, exc)
            break

def broadcast(text: str) -> None:
    for cid in CHAT_IDS:
        tg_send(cid, text)

# ──────────────────────────  ОБЪЯВЛЕНИЕ → ТЕКСТ  ─────────────────────────
def accept(o: dict) -> bool:
    try:
        rooms = int(o["rooms"])
    except (TypeError, ValueError):
        return False
    return rooms in ALLOWED_ROOMS and o["price"] <= MAX_PRICE

def msg(o: dict) -> str:
    """Ссылка первой строкой → Telegram формирует превью."""
    price = f"{o['price']:,}".replace(",", " ")
    return (
        f"{o['url']}\n"
        f"<b>{price} ₽</b> · {o['rooms']}-к, {o['area']} м²\n"
        f"{o['address']}"
    )

def process(o: dict, conn: sqlite3.Connection) -> None:
    """Сохраняем объявление и рассылаем тем чатам, где его ещё нет."""
    if not accept(o):
        return

    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO offers "
                "VALUES (:offer_id,:url,:price,:address,:area,:rooms,:date)", o)

    cur.execute("SELECT chat_id FROM sent WHERE offer_id=?", (o["offer_id"],))
    already = {row[0] for row in cur.fetchall()}
    for cid in (c for c in CHAT_IDS if c not in already):
        tg_send(cid, msg(o))
        cur.execute("INSERT OR IGNORE INTO sent VALUES (?,?)", (o["offer_id"], cid))
    conn.commit()

# ─────────────────────────────  C I A Н  ─────────────────────────────────
def cian_api() -> dict | None:
    q = {
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
                          data=json.dumps(q, ensure_ascii=False),
                          timeout=20)
        r.raise_for_status()
        return r.json()
    except Exception as exc:
        logging.error("[CIAN] %s", exc)
        return None

def cian_offer(it: dict) -> dict:
    return dict(
        url=it["fullUrl"],
        offer_id=it["id"],
        date=datetime.fromtimestamp(it["addedTimestamp"]).strftime("%Y-%m-%d %H:%M:%S"),
        price=it["bargainTerms"]["priceRur"],
        address=it["geo"]["userInput"],
        area=it["totalArea"],
        rooms=it["roomsCount"],
    )

def parse_cian(conn: sqlite3.Connection) -> None:
    js = cian_api()
    if not js:
        return
    for it in js["data"]["offersSerialized"]:
        process(cian_offer(it), conn)

# ─────────────────────────  YANDEX  REALTY  ──────────────────────────────
def ya_api() -> dict | None:
    providers = ["search", "filters", "searchParams", "seo", "queryId",
                 "forms", "filtersParams", "searchPresets", "react-search-data"]
    params = [("_providers", p) for p in providers] + [
        ("sort", "DATE_DESC"),
        ("rgid", "741964"),
        ("type", "RENT"),
        ("category", "APARTMENT"),
        ("agents", "NO"),
        ("_pageType", "search"),
        ("roomsTotalMin", "1"), ("roomsTotalMax", "1"),
        ("priceMax", str(MAX_PRICE)),
    ]
    back = (1, 3)
    for i in range(1, 6):
        try:
            r = requests.get("https://realty.yandex.ru/gate/react-page/get/",
                             headers=HEADERS, params=params, timeout=20)
            if 500 <= r.status_code < 600:
                raise requests.HTTPError(str(r.status_code), response=r)
            r.raise_for_status()
            return r.json()
        except Exception as exc:
            if i == 5:
                logging.error("[YA] %s", exc)
                return None
            pause = random.uniform(*back) * i
            logging.warning("[YA] попытка %s/5 — %s; пауза %.1f с", i, exc, pause)
            time.sleep(pause)

def ya_offer(it: dict) -> dict:
    raw = it.get("updateDate") or it["creationDate"]
    return dict(
        url=it["shareUrl"],
        offer_id=it["offerId"],
        date=raw.replace("T", " ").replace("Z", ""),
        price=it["price"]["value"],
        address=it["location"]["address"],
        area=it["area"]["value"],
        rooms=it["roomsTotalKey"],
    )

def parse_yandex(conn: sqlite3.Connection) -> None:
    js = ya_api()
    if not js:
        return
    for it in js["response"]["search"]["offers"]["entities"]:
        process(ya_offer(it), conn)

# ─────────────────────────────   MAIN   ──────────────────────────────────
def main() -> None:
    with db_conn() as conn:
        parse_cian(conn)
        parse_yandex(conn)

if __name__ == "__main__":
    main()
