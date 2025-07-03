# parser.py
# -*- coding: utf-8 -*-
"""
Парсер Циан + Яндекс с ГАРАНТИРОВАННЫМ устранением дубликатов.
• Каждый URL хешируется → абсолютная уникальность  
• Детальное логирование для отладки проблем с базой
• Двойная защита от повторных отправок
"""

from __future__ import annotations
from datetime import datetime
import hashlib, json, logging, os, random, sqlite3, time, urllib.parse
from typing import Dict, List

import requests

# ─────────── НАСТРОЙКИ ───────────
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
CHAT_IDS = [int(i) for i in os.getenv("CHAT_IDS", "").replace(" ", "").split(",") if i]
if not TG_BOT_TOKEN or not CHAT_IDS:
    raise RuntimeError("TG_BOT_TOKEN или CHAT_IDS не заданы")

MAX_PRICE, ALLOWED_ROOMS = 50_000, {1}
DB_FILE, MSG_DELAY = "offers.db", 1.0

logging.basicConfig(format="%(asctime)s  %(levelname)s  %(message)s",
                    level=logging.INFO)

HEADERS = {"user-agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/118.0 Safari/537.36")}
TG_URL = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"

# ─────────── ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ───────────
def url_hash(url: str) -> str:
    """Создаёт стабильный хеш URL для защиты от дубликатов."""
    # нормализуем URL: убираем query, fragment, лишние слеши
    p = urllib.parse.urlparse(url.strip())
    clean = p._replace(query="", fragment="").geturl()
    clean = clean.rstrip("/")
    # хешируем
    return hashlib.md5(clean.encode()).hexdigest()[:16]

def price_fmt(value: int) -> str:
    return f"{value:,}".replace(",", " ")

# ─────────── БАЗА ДАННЫХ ───────────
def db_conn() -> sqlite3.Connection:
    existed = os.path.exists(DB_FILE)
    conn = sqlite3.connect(DB_FILE, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL;")
    
    logging.info("📂 База %s %s", DB_FILE, "существовала" if existed else "создана новая")

    # схема с url_hash вместо url
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS offers(
            offer_id INTEGER PRIMARY KEY,
            url      TEXT UNIQUE,
            url_hash TEXT,
            price    INT,
            address  TEXT,
            area     REAL,
            rooms    INT,
            date     TEXT
        );
        CREATE TABLE IF NOT EXISTS sent(
            url_hash TEXT,
            chat_id  INTEGER,
            PRIMARY KEY (url_hash, chat_id)
        );
    """)

    # миграция: если в sent есть колонка url вместо url_hash
    cols = {row[1] for row in conn.execute("PRAGMA table_info(sent)")}
    if "url" in cols and "url_hash" not in cols:
        logging.warning("⟲ Миграция sent: url → url_hash")
        conn.executescript("""
            ALTER TABLE sent RENAME TO sent_old;
            CREATE TABLE sent(url_hash TEXT, chat_id INTEGER,
                              PRIMARY KEY (url_hash, chat_id));
            DROP TABLE sent_old;
        """)

    # статистика для отладки
    total_offers = conn.execute("SELECT COUNT(*) FROM offers").fetchone()[0]
    total_sent = conn.execute("SELECT COUNT(*) FROM sent").fetchone()[0]
    logging.info("📊 База: %d объявлений, %d записей отправки", total_offers, total_sent)
    
    return conn

# ─────────── TELEGRAM ───────────
_last_sent: Dict[int, float] = {}
_this_run_sent: set[str] = set()    # защита внутри одного прогона

def tg_send(chat: int, text: str) -> None:
    pause = MSG_DELAY - (time.time() - _last_sent.get(chat, 0))
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
                _last_sent[chat] = time.time()
                logging.info("✅ Отправлено в чат %s", chat)
            else:
                logging.error("❌ [TG %s] %s", chat, r.text)
            break
        except Exception as e:
            logging.error("❌ [TG %s] %s", chat, e)
            break

# ─────────── ОБРАБОТКА ОБЪЯВЛЕНИЙ ───────────
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

    url = o["url"]
    h = url_hash(url)
    o["url_hash"] = h

    cur = conn.cursor()
    
    # сохраняем объявление
    cur.execute("""INSERT OR IGNORE INTO offers
                   (offer_id,url,url_hash,price,address,area,rooms,date)
                   VALUES (:offer_id,:url,:url_hash,:price,:address,:area,:rooms,:date)""", o)

    # проверяем, в какие чаты уже отправляли этот url_hash
    cur.execute("SELECT chat_id FROM sent WHERE url_hash=?", (h,))
    already_sent = {row[0] for row in cur.fetchall()}
    
    logging.info("🔍 %s: хеш %s, уже отправлено в %s", 
                 url[:50] + "...", h, already_sent or "никуда")

    text = msg(o)
    sent_now = 0
    
    for cid in CHAT_IDS:
        key = f"{h}|{cid}"
        if cid in already_sent:
            logging.debug("⏭️  Чат %s уже получал %s", cid, h)
            continue
        if key in _this_run_sent:
            logging.debug("⏭️  Чат %s уже получил в этом прогоне %s", cid, h)
            continue

        tg_send(cid, text)
        _this_run_sent.add(key)
        cur.execute("INSERT OR IGNORE INTO sent VALUES (?,?)", (h, cid))
        sent_now += 1

    if sent_now > 0:
        logging.info("📤 Отправлено в %d чатов: %s", sent_now, url[:50] + "...")
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
            "date": datetime.fromtimestamp(i["addedTimestamp"]).strftime("%Y-%m-%d %H:%M:%S"),
            "price": i["bargainTerms"]["priceRur"],
            "address": i["geo"]["userInput"],
            "area": i["totalArea"],
            "rooms": i["roomsCount"]}

def parse_cian(c: sqlite3.Connection) -> None:
    j = cian_api()
    if j:
        offers = j["data"]["offersSerialized"]
        logging.info("[CIAN] Получено %d объявлений", len(offers))
        for it in offers:
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
        offers = j["response"]["search"]["offers"]["entities"]
        logging.info("[YA] Получено %d объявлений", len(offers))
        for it in offers:
            process(ya_offer(it), c)

# ─────────── MAIN ───────────
def main() -> None:
    logging.info("🚀 Запуск парсера")
    with db_conn() as conn:
        parse_cian(conn)
        parse_yandex(conn)
    logging.info("✅ Парсер завершён")

if __name__ == "__main__":
    main()
