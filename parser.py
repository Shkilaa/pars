# parser.py
# -*- coding: utf-8 -*-
"""
Парсер Циан + Яндекс (1-к ≤ 50 000 ₽) с рассылкой в Telegram.
— ссылка первой строкой → Telegram показывает карточку-превью;
— дублей нет: для каждого chat_id храним «очищенный» URL объявления.
"""

from __future__ import annotations
from datetime import datetime
import json, logging, os, random, sqlite3, time, urllib.parse
from typing import Dict, List

import requests

# ─────────── КОНСТАНТЫ ───────────
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
CHAT_IDS = [int(i) for i in os.getenv("CHAT_IDS", "").replace(" ", "").split(",") if i]
if not TG_BOT_TOKEN or not CHAT_IDS:
    raise RuntimeError("TG_BOT_TOKEN или CHAT_IDS не заданы")

MAX_PRICE, ALLOWED_ROOMS = 50_000, {1}
DB_FILE, MSG_DELAY = "offers.db", 1.0

logging.basicConfig(format="%(asctime)s  %(levelname)s  %(message)s",
                    level=logging.INFO)

HEADERS = {"user-agent":
           ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/118.0 Safari/537.36")}
TG_URL = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"

# ─────────── ВСПОМОГАТЕЛЬНОЕ ───────────
def norm(url: str) -> str:
    """Удаляем query, fragment, двойной // и конечный /."""
    p = urllib.parse.urlparse(url)
    clean = p._replace(query="", fragment="").geturl()
    if clean.endswith("/"):   # https://site/obj/ → https://site/obj
        clean = clean[:-1]
    return clean

def price_fmt(value: int) -> str:
    return f"{value:,}".replace(",", " ")

# ─────────── БАЗА ───────────
def db_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL;")

    # актуальная схема
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS offers(
            offer_id INTEGER PRIMARY KEY,
            url      TEXT UNIQUE,
            price    INT,
            address  TEXT,
            area     REAL,
            rooms    INT,
            date     TEXT
        );
        CREATE TABLE IF NOT EXISTS sent(
            url     TEXT,
            chat_id INTEGER,
            PRIMARY KEY (url, chat_id)
        );
    """)

    # миграция: если в sent нет колонки url → пересоздаём таблицу
    cols = {row[1] for row in conn.execute("PRAGMA table_info(sent)")}
    if "url" not in cols:
        logging.warning("⟲ миграция таблицы sent на (url, chat_id)")
        conn.executescript("""
            ALTER TABLE sent RENAME TO sent_old;
            CREATE TABLE sent(url TEXT, chat_id INTEGER,
                              PRIMARY KEY (url, chat_id));
            DROP TABLE sent_old;
        """)
    return conn

# ─────────── TELEGRAM ───────────
_last: Dict[int, float] = {}
_sent_run: set[str] = set()

def tg_send(chat: int, text: str) -> None:
    pause = MSG_DELAY - (time.time() - _last.get(chat, 0))
    if pause > 0:
        time.sleep(pause)

    while True:
        try:
            r = requests.post(TG_URL,
                              data={"chat_id": chat,
                                    "text": text,
                                    "parse_mode": "HTML",
                                    "disable_web_page_preview": False},
                              timeout=10)
            if r.status_code == 429:
                retry = r.json()["parameters"]["retry_after"]
                logging.warning("429 для %s, пауза %s c", chat, retry)
                time.sleep(retry)
                continue
            if r.ok:
                _last[chat] = time.time()
                logging.info("Отправлено в %s", chat)
            else:
                logging.error("[TG %s] %s", chat, r.text)
            break
        except Exception as e:
            logging.error("[TG %s] %s", chat, e)
            break

# ─────────── ОБРАБОТКА ОБЪЯВЛЕНИЯ ───────────
def accept(o: dict) -> bool:
    try:
        rooms = int(o["rooms"])
    except (TypeError, ValueError):
        return False
    return rooms in ALLOWED_ROOMS and o["price"] <= MAX_PRICE

def msg(o: dict) -> str:
    return (f"{o['url']}\n"
            f"<b>{price_fmt(o['price'])} ₽</b> · {o['rooms']}-к, {o['area']} м²\n"
            f"{o['address']}")

def process(o: dict, conn: sqlite3.Connection) -> None:
    if not accept(o):
        return

    o["url"] = norm(o["url"])          # канонизируем ссылку
    url = o["url"]

    cur = conn.cursor()
    cur.execute("""INSERT OR IGNORE INTO offers
                   (offer_id,url,price,address,area,rooms,date)
                   VALUES (:offer_id,:url,:price,:address,:area,:rooms,:date)""", o)

    cur.execute("SELECT chat_id FROM sent WHERE url=?", (url,))
    done = {r[0] for r in cur.fetchall()}

    txt = msg(o)
    for cid in CHAT_IDS:
        key = f"{url}|{cid}"
        if cid in done or key in _sent_run:
            continue
        tg_send(cid, txt)
        _sent_run.add(key)
        cur.execute("INSERT OR IGNORE INTO sent VALUES (?,?)", (url, cid))
    conn.commit()

# ─────────── ЦИАН ───────────
def cian_api() -> dict | None:
    q = {"jsonQuery": {"region": {"type": "terms", "value": [1]},
                       "_type": "flatrent",
                       "room": {"type": "terms", "value": [1]},
                       "engine_version": {"type": "term", "value": 2},
                       "for_day": {"type": "term", "value": "!1"},
                       "is_by_homeowner": {"type": "term", "value": True},
                       "sort": {"type": "term", "value": "creation_date_desc"},
                       "bargain_terms": {"type": "range", "value": {"lte": MAX_PRICE}}}}
    try:
        r = requests.post("https://api.cian.ru/search-offers/v2/search-offers-desktop/",
                          headers=HEADERS, data=json.dumps(q, ensure_ascii=False),
                          timeout=20)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logging.error("[CIAN] %s", e)
        return None

def cian_offer(i: dict) -> dict:
    return {"url": i["fullUrl"],
            "offer_id": i["id"],
            "date": datetime.fromtimestamp(i["addedTimestamp"])
                     .strftime("%Y-%m-%d %H:%M:%S"),
            "price": i["bargainTerms"]["priceRur"],
            "address": i["geo"]["userInput"],
            "area": i["totalArea"],
            "rooms": i["roomsCount"]}

def parse_cian(c: sqlite3.Connection) -> None:
    j = cian_api()
    if j:
        for it in j["data"]["offersSerialized"]:
            process(cian_offer(it), c)

# ─────────── YANDEX ───────────
def ya_api() -> dict | None:
    prov = ["search","filters","searchParams","seo","queryId",
            "forms","filtersParams","searchPresets","react-search-data"]
    params = [("_providers", p) for p in prov] + [
        ("sort", "DATE_DESC"), ("rgid", "741964"), ("type", "RENT"),
        ("category", "APARTMENT"), ("agents", "NO"), ("_pageType", "search"),
        ("roomsTotalMin", "1"), ("roomsTotalMax", "1"),
        ("priceMax", str(MAX_PRICE))]
    for i in range(5):
        try:
            r = requests.get("https://realty.yandex.ru/gate/react-page/get/",
                             headers=HEADERS, params=params, timeout=20)
            if 500 <= r.status_code < 600:
                raise requests.HTTPError(str(r.status_code), response=r)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if i == 4:
                logging.error("[YA] %s", e)
                return None
            time.sleep(random.uniform(1, 3) * (i + 1))

def ya_offer(i: dict) -> dict:
    raw = i.get("updateDate") or i["creationDate"]
    return {"url": i["shareUrl"],
            "offer_id": i["offerId"],
            "date": raw.replace("T", " ").replace("Z", ""),
            "price": i["price"]["value"],
            "address": i["location"]["address"],
            "area": i["area"]["value"],
            "rooms": i["roomsTotalKey"]}

def parse_yandex(c: sqlite3.Connection) -> None:
    j = ya_api()
    if j:
        for it in j["response"]["search"]["offers"]["entities"]:
            process(ya_offer(it), c)

# ─────────── MAIN ───────────
def main() -> None:
    with db_conn() as conn:
        parse_cian(conn)
        parse_yandex(conn)

if __name__ == "__main__":
    main()
