# parser.py
# -*- coding: utf-8 -*-
"""
Парсер 1-комнатных квартир (≤ 50 000 ₽) с Циана и Яндекс.Недвижимости
с рассылкой в Telegram-чаты.

• URL объявления идёт первой строкой → Telegram показывает превью-карточку
• Каждое объявление отправляется в конкретный чат строго ОДИН раз
• Нормализация URL убирает дубликаты от query-параметров
• Автоматическая миграция базы данных при обновлении структуры
"""

from __future__ import annotations
from datetime import datetime
import json
import logging
import os
import random
import sqlite3
import time
import urllib.parse
from typing import Dict, List

import requests

# ────────────────────────────  ПАРАМЕТРЫ  ────────────────────────────────
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
CHAT_IDS = [int(x) for x in os.getenv("CHAT_IDS", "").replace(" ", "").split(",") if x]

if not TG_BOT_TOKEN or not CHAT_IDS:
    raise RuntimeError("TG_BOT_TOKEN или CHAT_IDS не заданы в переменных окружения")

MAX_PRICE = 50_000
ALLOWED_ROOMS = {1}

DB_FILE = "offers.db"
MSG_DELAY = 1.0  # сек между личными сообщениями

logging.basicConfig(
    format="%(asctime)s  %(levelname)s  %(message)s",
    level=logging.INFO
)

HEADERS = {
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/118.0 Safari/537.36"
    )
}
TG_URL = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"

# ──────────────────────  НОРМАЛИЗАЦИЯ URL  ───────────────────────────────
def canon(url: str) -> str:
    """Приводим ссылку к каноническому виду без query-параметров."""
    try:
        p = urllib.parse.urlparse(url)
        netloc = p.netloc.lower().lstrip("www.")
        
        # сводим поддомены к основным доменам
        if netloc.endswith(".cian.ru"):
            netloc = "cian.ru"
        elif netloc.endswith(".yandex.ru"):
            netloc = "realty.yandex.ru"
            
        path = p.path.rstrip("/").lower()
        return f"https://{netloc}{path}"
    except Exception:
        return url  # если что-то пошло не так, возвращаем как есть

# ─────────────────────────────  БАЗА  ────────────────────────────────────
def db_conn() -> sqlite3.Connection:
    """Создаёт соединение с автоматической миграцией структуры."""
    conn = sqlite3.connect(DB_FILE, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL;")
    
    # проверяем существующую структуру offers
    try:
        cur = conn.execute("PRAGMA table_info(offers);")
        offers_cols = {row[1] for row in cur.fetchall()}
    except:
        offers_cols = set()
    
    # проверяем существующую структуру sent
    try:
        cur = conn.execute("PRAGMA table_info(sent);")
        sent_cols = {row[1] for row in cur.fetchall()}
    except:
        sent_cols = set()
    
    # миграция offers: если нет колонки url или таблицы вообще нет
    if "url" not in offers_cols:
        if offers_cols:  # таблица есть, но без url
            logging.warning("⟲ добавляем колонку url в таблицу offers")
            try:
                conn.execute("ALTER TABLE offers ADD COLUMN url TEXT;")
            except sqlite3.OperationalError:
                pass  # колонка уже есть
        else:  # таблицы нет
            logging.info("Создаём таблицу offers")
            conn.execute("""
                CREATE TABLE offers(
                    offer_id INTEGER PRIMARY KEY,
                    url      TEXT UNIQUE,
                    price    INT,
                    address  TEXT,
                    area     REAL,
                    rooms    INT,
                    date     TEXT
                );
            """)
    
    # миграция sent: пересоздаём, если структура не (url, chat_id)
    if sent_cols != {"url", "chat_id"}:
        logging.warning("⟲ пересоздаём таблицу sent с новой структурой")
        conn.executescript("""
            DROP TABLE IF EXISTS sent_old;
            DROP TABLE IF EXISTS sent;
            CREATE TABLE sent(
                url     TEXT,
                chat_id INTEGER,
                PRIMARY KEY (url, chat_id)
            );
        """)
    
    return conn

# ───────────────────────  ОТПРАВКА В TELEGRAM  ──────────────────────────
_last_sent: Dict[int, float] = {}
_sent_this_run: set[str] = set()

def tg_send(chat: int, text: str) -> None:
    """Отправляем сообщение в один чат с учётом лимитов."""
    pause = MSG_DELAY - (time.time() - _last_sent.get(chat, 0))
    if pause > 0:
        time.sleep(pause)

    while True:
        try:
            r = requests.post(
                TG_URL,
                data={
                    "chat_id": chat,
                    "text": text,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": False  # показываем превью
                },
                timeout=10,
            )

            if r.status_code == 429:
                retry = r.json()["parameters"]["retry_after"]
                logging.warning("429 для %s, пауза %s c", chat, retry)
                time.sleep(retry)
                continue

            if r.ok:
                _last_sent[chat] = time.time()
                logging.info("Отправлено в чат %s", chat)
            else:
                logging.error("[TG %s] %s", chat, r.text)
            break
        except Exception as exc:
            logging.error("[TG %s] %s", chat, exc)
            break

# ──────────────────────  ОБРАБОТКА ОБЪЯВЛЕНИЙ  ───────────────────────────
def accept_offer(offer: dict) -> bool:
    """Проверяем, подходит ли объявление по критериям."""
    try:
        rooms = int(offer["rooms"])
    except (TypeError, ValueError):
        return False
    return rooms in ALLOWED_ROOMS and offer["price"] <= MAX_PRICE

def format_message(offer: dict) -> str:
    """Форматируем сообщение: URL первой строкой для превью."""
    price = f"{offer['price']:,}".replace(",", " ")
    return (
        f"{offer['url']}\n"
        f"<b>{price} ₽</b> · {offer['rooms']}-к, {offer['area']} м²\n"
        f"{offer['address']}"
    )

def process_offer(offer: dict, conn: sqlite3.Connection) -> None:
    """Обрабатываем объявление: сохраняем и рассылаем новым чатам."""
    if not accept_offer(offer):
        return

    # нормализуем URL
    offer["url"] = canon(offer["url"])
    url = offer["url"]

    cur = conn.cursor()
    
    # сохраняем объявление
    try:
        cur.execute("""
            INSERT OR IGNORE INTO offers
            (offer_id, url, price, address, area, rooms, date)
            VALUES (:offer_id, :url, :price, :address, :area, :rooms, :date)
        """, offer)
    except sqlite3.OperationalError as e:
        logging.error("Ошибка сохранения объявления: %s", e)
        return

    # узнаём, в какие чаты уже отправляли
    cur.execute("SELECT chat_id FROM sent WHERE url=?", (url,))
    already_sent = {row[0] for row in cur.fetchall()}

    # рассылаем в новые чаты
    text = format_message(offer)
    for chat_id in CHAT_IDS:
        key = f"{url}|{chat_id}"
        if chat_id in already_sent or key in _sent_this_run:
            continue

        tg_send(chat_id, text)
        _sent_this_run.add(key)
        cur.execute("INSERT OR IGNORE INTO sent VALUES (?, ?)", (url, chat_id))

    conn.commit()

# ─────────────────────────────  ЦИАН  ────────────────────────────────────
def get_cian_data() -> dict | None:
    """Получаем данные с API Циана."""
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
        r = requests.post(
            "https://api.cian.ru/search-offers/v2/search-offers-desktop/",
            headers=HEADERS,
            data=json.dumps(query, ensure_ascii=False),
            timeout=20,
        )
        r.raise_for_status()
        return r.json()
    except Exception as exc:
        logging.error("[CIAN] %s", exc)
        return None

def parse_cian_offer(item: dict) -> dict:
    """Парсим объявление Циана в стандартный формат."""
    return {
        "url": item["fullUrl"],
        "offer_id": item["id"],
        "date": datetime.fromtimestamp(item["addedTimestamp"]).strftime("%Y-%m-%d %H:%M:%S"),
        "price": item["bargainTerms"]["priceRur"],
        "address": item["geo"]["userInput"],
        "area": item["totalArea"],
        "rooms": item["roomsCount"],
    }

def parse_cian(conn: sqlite3.Connection) -> None:
    """Парсим объявления с Циана."""
    data = get_cian_data()
    if data:
        for item in data["data"]["offersSerialized"]:
            process_offer(parse_cian_offer(item), conn)

# ─────────────────────────  YANDEX  REALTY  ──────────────────────────────
def get_yandex_data() -> dict | None:
    """Получаем данные с API Яндекс.Недвижимости."""
    providers = [
        "search", "filters", "searchParams", "seo", "queryId",
        "forms", "filtersParams", "searchPresets", "react-search-data"
    ]
    params = [("_providers", p) for p in providers] + [
        ("sort", "DATE_DESC"),
        ("rgid", "741964"),
        ("type", "RENT"),
        ("category", "APARTMENT"),
        ("agents", "NO"),
        ("_pageType", "search"),
        ("roomsTotalMin", "1"),
        ("roomsTotalMax", "1"),
        ("priceMax", str(MAX_PRICE)),
    ]
    
    for attempt in range(5):
        try:
            r = requests.get(
                "https://realty.yandex.ru/gate/react-page/get/",
                headers=HEADERS,
                params=params,
                timeout=20,
            )
            if 500 <= r.status_code < 600:
                raise requests.HTTPError(f"{r.status_code}", response=r)
            r.raise_for_status()
            return r.json()
        except Exception as exc:
            if attempt == 4:
                logging.error("[YA] %s", exc)
                return None
            pause = random.uniform(1, 3) * (attempt + 1)
            logging.warning("[YA] попытка %s/5 — %s, пауза %.1f c", attempt + 1, exc, pause)
            time.sleep(pause)

def parse_yandex_offer(item: dict) -> dict:
    """Парсим объявление Яндекса в стандартный формат."""
    date_raw = item.get("updateDate") or item["creationDate"]
    return {
        "url": item["shareUrl"],
        "offer_id": item["offerId"],
        "date": date_raw.replace("T", " ").replace("Z", ""),
        "price": item["price"]["value"],
        "address": item["location"]["address"],
        "area": item["area"]["value"],
        "rooms": item["roomsTotalKey"],
    }

def parse_yandex(conn: sqlite3.Connection) -> None:
    """Парсим объявления с Яндекс.Недвижимости."""
    data = get_yandex_data()
    if data:
        for item in data["response"]["search"]["offers"]["entities"]:
            process_offer(parse_yandex_offer(item), conn)

# ─────────────────────────────   MAIN   ──────────────────────────────────
def main() -> None:
    """Основная функция: парсим оба сайта."""
    with db_conn() as conn:
        parse_cian(conn)
        parse_yandex(conn)

if __name__ == "__main__":
    main()
