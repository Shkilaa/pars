# parser.py
# -*- coding: utf-8 -*-
"""
Парсер 1-комнатных квартир (≤ 50 000 ₽) с Циана и Яндекс.Недвижимости
с рассылкой в Telegram-чаты.
• URL объявления идёт первой строкой → Telegram показывает превью-карточку
• Каждое объявление отправляется в конкретный чат строго ОДИН раз
• Нормализация URL убирает дубликаты от query-параметров
• Автоматическая миграция базы данных при обновлении структуры
• Защита от дубликатов по содержимому объявлений
"""

from __future__ import annotations
from datetime import datetime, timedelta
import hashlib
import json
import logging
import os
import random
import sqlite3
import time
import urllib.parse
from typing import Dict, List
import requests

# ──────────────────────────── ПАРАМЕТРЫ ────────────────────────────────
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
CHAT_IDS = [int(x) for x in os.getenv("CHAT_IDS", "").replace(" ", "").split(",") if x]

if not TG_BOT_TOKEN or not CHAT_IDS:
    raise RuntimeError("TG_BOT_TOKEN или CHAT_IDS не заданы в переменных окружения")

MAX_PRICE = 50_000
ALLOWED_ROOMS = {1}
DB_FILE = "offers.db"
MSG_DELAY = 1.0  # сек между личными сообщениями
CLEANUP_DAYS = 30  # дни для очистки старых объявлений

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
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

# ────────────────────── НОРМАЛИЗАЦИЯ URL ───────────────────────────────
def canon(url: str) -> str:
    """Улучшенная нормализация URL с извлечением ID объявлений."""
    try:
        p = urllib.parse.urlparse(url)
        netloc = p.netloc.lower().lstrip("www.")
        
        # Сводим поддомены к основным доменам
        if netloc.endswith(".cian.ru"):
            netloc = "cian.ru"
        elif netloc.endswith(".yandex.ru"):
            netloc = "realty.yandex.ru"
        
        path = p.path.rstrip("/").lower()
        
        # Извлекаем ID для более точной идентификации
        if "cian.ru" in netloc:
            # Для Циана ID обычно в конце пути
            path_parts = path.split('/')
            if path_parts and path_parts[-1].isdigit():
                return f"https://{netloc}/rent/flat/{path_parts[-1]}/"
        elif "yandex.ru" in netloc:
            # Для Яндекса ID может быть в разных местах
            path_parts = path.split('/')
            for part in path_parts:
                if part.isdigit() and len(part) > 5:  # ID обычно длинный
                    return f"https://{netloc}/offer/{part}/"
        
        return f"https://{netloc}{path}"
    except Exception:
        return url

# ───────────────────────────── БАЗА ────────────────────────────────────
def create_content_hash(offer: dict) -> str:
    """Создаем хеш на основе ключевых характеристик объявления."""
    # Нормализуем адрес для более точного сравнения
    address = offer['address'].lower().strip()
    content = f"{offer['price']}_{offer['rooms']}_{offer['area']:.1f}_{address}"
    return hashlib.md5(content.encode('utf-8')).hexdigest()

def db_conn() -> sqlite3.Connection:
    """Создаёт соединение с улучшенной структурой для предотвращения дубликатов."""
    conn = sqlite3.connect(DB_FILE, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL;")
    
    # Проверяем существующую структуру offers
    try:
        cur = conn.execute("PRAGMA table_info(offers);")
        offers_cols = {row[1] for row in cur.fetchall()}
    except:
        offers_cols = set()
    
    # Проверяем существующую структуру sent
    try:
        cur = conn.execute("PRAGMA table_info(sent);")
        sent_cols = {row[1] for row in cur.fetchall()}
    except:
        sent_cols = set()
    
    # Миграция offers: добавляем новые колонки
    if "content_hash" not in offers_cols:
        if offers_cols:  # Таблица есть, добавляем колонки
            logging.warning("⟲ добавляем колонки content_hash и source в таблицу offers")
            try:
                conn.execute("ALTER TABLE offers ADD COLUMN content_hash TEXT;")
                conn.execute("ALTER TABLE offers ADD COLUMN source TEXT;")
            except sqlite3.OperationalError:
                pass  # Колонки уже есть
        else:  # Таблицы нет, создаем новую
            logging.info("Создаём таблицу offers с новой структурой")
            conn.execute("""
                CREATE TABLE offers(
                    offer_id INTEGER PRIMARY KEY,
                    url TEXT UNIQUE,
                    content_hash TEXT,
                    price INT,
                    address TEXT,
                    area REAL,
                    rooms INT,
                    date TEXT,
                    source TEXT
                );
            """)
    
    # Создаем индексы для быстрого поиска дубликатов
    conn.executescript("""
        CREATE INDEX IF NOT EXISTS idx_content_hash ON offers(content_hash);
        CREATE INDEX IF NOT EXISTS idx_price_rooms_area ON offers(price, rooms, area);
        CREATE INDEX IF NOT EXISTS idx_source_date ON offers(source, date);
    """)
    
    # Миграция sent: пересоздаём, если структура не (url, chat_id)
    if sent_cols != {"url", "chat_id", "sent_date"}:
        logging.warning("⟲ пересоздаём таблицу sent с новой структурой")
        conn.executescript("""
            DROP TABLE IF EXISTS sent_old;
            DROP TABLE IF EXISTS sent;
            CREATE TABLE sent(
                url TEXT,
                chat_id INTEGER,
                sent_date TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (url, chat_id)
            );
        """)
    
    return conn

def cleanup_old_offers(conn: sqlite3.Connection) -> None:
    """Удаляем старые объявления для экономии места."""
    cutoff_date = (datetime.now() - timedelta(days=CLEANUP_DAYS)).isoformat()
    cur = conn.cursor()
    
    # Удаляем старые объявления
    cur.execute("DELETE FROM offers WHERE date < ?", (cutoff_date,))
    deleted_offers = cur.rowcount
    
    # Удаляем записи об отправке для несуществующих объявлений
    cur.execute("DELETE FROM sent WHERE url NOT IN (SELECT url FROM offers)")
    deleted_sent = cur.rowcount
    
    conn.commit()
    
    if deleted_offers > 0 or deleted_sent > 0:
        logging.info("Очищено: %s объявлений, %s записей отправки", deleted_offers, deleted_sent)

# ─────────────────────── ОТПРАВКА В TELEGRAM ──────────────────────────
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

# ────────────────────── ОБРАБОТКА ОБЪЯВЛЕНИЙ ───────────────────────────
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
        f"{price} ₽ · {offer['rooms']}-к, {offer['area']} м²\n"
        f"{offer['address']}"
    )

def process_offer(offer: dict, conn: sqlite3.Connection) -> None:
    """Улучшенная обработка объявления с защитой от дубликатов."""
    if not accept_offer(offer):
        return
    
    # Нормализуем URL и создаем хеш содержимого
    offer["url"] = canon(offer["url"])
    url = offer["url"]
    content_hash = create_content_hash(offer)
    source = 'cian' if 'cian.ru' in url else 'yandex'
    
    cur = conn.cursor()
    
    # Комплексная проверка дубликатов
    cur.execute("""
        SELECT offer_id, url FROM offers 
        WHERE url = ? OR content_hash = ? OR 
        (price = ? AND rooms = ? AND ABS(area - ?) < 1 AND LOWER(address) = LOWER(?))
        LIMIT 1
    """, (url, content_hash, offer['price'], offer['rooms'], offer['area'], offer['address']))
    
    existing = cur.fetchone()
    if existing:
        logging.info("Дубликат обнаружен (ID: %s, URL: %s), пропускаем: %s", 
                    existing[0], existing[1], url)
        return
    
    # Сохраняем новое объявление
    try:
        cur.execute("""
            INSERT INTO offers
            (offer_id, url, content_hash, price, address, area, rooms, date, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (offer['offer_id'], url, content_hash, offer['price'], 
              offer['address'], offer['area'], offer['rooms'], offer['date'], source))
        
        # Узнаём, в какие чаты уже отправляли
        cur.execute("SELECT chat_id FROM sent WHERE url=?", (url,))
        already_sent = {row[0] for row in cur.fetchall()}
        
        # Рассылаем в новые чаты
        text = format_message(offer)
        new_chats = 0
        
        for chat_id in CHAT_IDS:
            key = f"{url}|{chat_id}"
            if chat_id in already_sent or key in _sent_this_run:
                continue
            
            tg_send(chat_id, text)
            _sent_this_run.add(key)
            cur.execute("INSERT OR IGNORE INTO sent VALUES (?, ?, ?)", 
                       (url, chat_id, datetime.now().isoformat()))
            new_chats += 1
        
        conn.commit()
        logging.info("Новое объявление добавлено и отправлено в %s чатов: %s", new_chats, url)
        
    except sqlite3.IntegrityError:
        logging.warning("Объявление уже существует в базе: %s", url)
    except sqlite3.OperationalError as e:
        logging.error("Ошибка сохранения объявления: %s", e)

# ───────────────────────────── ЦИАН ────────────────────────────────────
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
        processed = 0
        for item in data["data"]["offersSerialized"]:
            process_offer(parse_cian_offer(item), conn)
            processed += 1
        logging.info("Обработано объявлений с Циана: %s", processed)

# ───────────────────────── YANDEX REALTY ──────────────────────────────
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
        processed = 0
        for item in data["response"]["search"]["offers"]["entities"]:
            process_offer(parse_yandex_offer(item), conn)
            processed += 1
        logging.info("Обработано объявлений с Яндекса: %s", processed)

# ───────────────────────────── MAIN ──────────────────────────────────
def main() -> None:
    """Основная функция с улучшенной статистикой."""
    logging.info("Запуск парсера в %s", datetime.now())
    
    with db_conn() as conn:
        # Очистка старых записей
        cleanup_old_offers(conn)
        
        # Статистика до парсинга
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM offers")
        offers_before = cur.fetchone()[0]
        
        # Парсинг
        logging.info("Парсинг Циан...")
        parse_cian(conn)
        
        logging.info("Парсинг Яндекс...")
        parse_yandex(conn)
        
        # Статистика после парсинга
        cur.execute("SELECT COUNT(*) FROM offers")
        offers_after = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(DISTINCT url) FROM sent")
        sent_offers = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM offers WHERE source = 'cian'")
        cian_offers = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM offers WHERE source = 'yandex'")
        yandex_offers = cur.fetchone()[0]
        
        new_offers = offers_after - offers_before
        
        logging.info("Статистика: новых объявлений: %s, всего: %s (Циан: %s, Яндекс: %s), отправлено: %s", 
                    new_offers, offers_after, cian_offers, yandex_offers, sent_offers)

if __name__ == "__main__":
    main()
