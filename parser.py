# parser.py
# -*- coding: utf-8 -*-
"""
Парсер 1-комнатных квартир (≤ 50 000 ₽) с Циана и Яндекс.Недвижимости
с рассылкой в Telegram-чаты и расчетом времени в пути на общественном транспорте.
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
from typing import Dict, List, Optional
import requests

# ──────────────────────────── ПАРАМЕТРЫ ────────────────────────────────
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
CHAT_IDS = [int(x) for x in os.getenv("CHAT_IDS", "").replace(" ", "").split(",") if x]
YANDEX_GEOCODER_API_KEY = os.getenv("YANDEX_GEOCODER_API_KEY")
DESTINATION_ADDRESS = os.getenv("DESTINATION_ADDRESS", "Москва, Остаповский проезд, 22с16")

if not TG_BOT_TOKEN or not CHAT_IDS:
    raise RuntimeError("TG_BOT_TOKEN или CHAT_IDS не заданы в переменных окружения")

if not YANDEX_GEOCODER_API_KEY:
    logging.warning("YANDEX_GEOCODER_API_KEY не задан - время в пути не будет рассчитываться")

MAX_PRICE = 50_000
ALLOWED_ROOMS = {1}
DB_FILE = "offers.db"
MSG_DELAY = 1.0
CLEANUP_DAYS = 30

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

# ────────────────────── YANDEX MAPS INTEGRATION ───────────────────────────
def get_coordinates(address: str) -> Optional[tuple]:
    """Получаем координаты адреса через Yandex Geocoder API."""
    if not YANDEX_GEOCODER_API_KEY:
        return None
    
    try:
        params = {
            'apikey': YANDEX_GEOCODER_API_KEY,
            'geocode': address,
            'format': 'json',
            'results': 1
        }
        
        response = requests.get(
            'https://geocode-maps.yandex.ru/1.x/',
            params=params,
            timeout=10
        )
        
        if response.status_code != 200:
            logging.warning("Geocoder API вернул статус %s для адреса: %s", response.status_code, address)
            return None
            
        data = response.json()
        
        try:
            pos = data['response']['GeoObjectCollection']['featureMember'][0]['GeoObject']['Point']['pos']
            lon, lat = pos.split()
            logging.info("Координаты для '%s': %s, %s", address, lat, lon)
            return (float(lat), float(lon))
        except (KeyError, IndexError, ValueError) as e:
            logging.warning("Не удалось извлечь координаты для адреса '%s': %s", address, e)
            return None
            
    except Exception as e:
        logging.error("Ошибка геокодирования для адреса '%s': %s", address, e)
        return None

def get_travel_time_simple(origin_address: str, destination_address: str) -> Optional[str]:
    """Упрощенный расчет времени в пути через координаты."""
    if not YANDEX_GEOCODER_API_KEY:
        return None
    
    try:
        # Получаем координаты обоих адресов
        origin_coords = get_coordinates(origin_address)
        dest_coords = get_coordinates(destination_address)
        
        if not origin_coords or not dest_coords:
            logging.warning("Не удалось получить координаты для маршрута: %s -> %s", origin_address, destination_address)
            return None
        
        # Простой расчет расстояния и примерного времени
        import math
        
        # Формула гаверсинуса для расчета расстояния
        lat1, lon1 = math.radians(origin_coords[0]), math.radians(origin_coords[1])
        lat2, lon2 = math.radians(dest_coords[0]), math.radians(dest_coords[1])
        
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        
        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
        c = 2 * math.asin(math.sqrt(a))
        distance_km = 6371 * c
        
        # Примерное время на общественном транспорте (средняя скорость 25 км/ч)
        travel_time_hours = distance_km / 25
        travel_time_minutes = round(travel_time_hours * 60)
        
        if travel_time_minutes < 60:
            return f"{travel_time_minutes} мин"
        else:
            hours = travel_time_minutes // 60
            minutes = travel_time_minutes % 60
            return f"{hours}ч {minutes}мин"
        
    except Exception as e:
        logging.error("Ошибка расчета времени в пути: %s", e)
        return None

def get_travel_time(origin_address: str, destination_address: str) -> Optional[str]:
    """Получаем время в пути на общественном транспорте."""
    if not YANDEX_GEOCODER_API_KEY:
        return None
    
    try:
        # Сначала пробуем API маршрутизации
        origin_coords = get_coordinates(origin_address)
        dest_coords = get_coordinates(destination_address)
        
        if not origin_coords or not dest_coords:
            return None
        
        # Пробуем Yandex Router API
        waypoints = f"{origin_coords[0]},{origin_coords[1]}|{dest_coords[0]},{dest_coords[1]}"
        
        params = {
            'apikey': YANDEX_GEOCODER_API_KEY,
            'waypoints': waypoints,
            'mode': 'transit',
            'format': 'json'
        }
        
        response = requests.get(
            'https://api.routing.yandex.net/v2/route',
            params=params,
            timeout=15
        )
        
        if response.status_code == 200:
            data = response.json()
            
            if 'route' in data and 'legs' in data['route']:
                total_duration = 0
                for leg in data['route']['legs']:
                    if 'duration' in leg:
                        total_duration += leg['duration']
                
                if total_duration > 0:
                    duration_minutes = round(total_duration / 60)
                    if duration_minutes < 60:
                        return f"{duration_minutes} мин"
                    else:
                        hours = duration_minutes // 60
                        minutes = duration_minutes % 60
                        return f"{hours}ч {minutes}мин"
        
        # Если API маршрутизации не работает, используем простой расчет
        logging.info("Используем простой расчет времени для %s", origin_address)
        return get_travel_time_simple(origin_address, destination_address)
        
    except Exception as e:
        logging.error("Ошибка расчета маршрута: %s", e)
        # Fallback на простой расчет
        return get_travel_time_simple(origin_address, destination_address)

# ────────────────────── НОРМАЛИЗАЦИЯ URL ───────────────────────────────
def canon(url: str) -> str:
    """Улучшенная нормализация URL с извлечением ID объявлений."""
    try:
        p = urllib.parse.urlparse(url)
        netloc = p.netloc.lower().lstrip("www.")
        
        if netloc.endswith(".cian.ru"):
            netloc = "cian.ru"
        elif netloc.endswith(".yandex.ru"):
            netloc = "realty.yandex.ru"
        
        path = p.path.rstrip("/").lower()
        
        if "cian.ru" in netloc:
            path_parts = path.split('/')
            if path_parts and path_parts[-1].isdigit():
                return f"https://{netloc}/rent/flat/{path_parts[-1]}/"
        elif "yandex.ru" in netloc:
            path_parts = path.split('/')
            for part in path_parts:
                if part.isdigit() and len(part) > 5:
                    return f"https://{netloc}/offer/{part}/"
        
        return f"https://{netloc}{path}"
    except Exception:
        return url

# ───────────────────────────── БАЗА ────────────────────────────────────
def create_content_hash(offer: dict) -> str:
    """Создаем хеш на основе ключевых характеристик объявления."""
    address = str(offer['address']).lower().strip()
    
    try:
        area = float(offer['area'])
        area_str = f"{area:.1f}"
    except (ValueError, TypeError):
        area_str = str(offer['area'])
    
    content = f"{offer['price']}_{offer['rooms']}_{area_str}_{address}"
    return hashlib.md5(content.encode('utf-8')).hexdigest()

def db_conn() -> sqlite3.Connection:
    """Создаёт соединение с улучшенной структурой для предотвращения дубликатов."""
    conn = sqlite3.connect(DB_FILE, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL;")
    
    try:
        cur = conn.execute("PRAGMA table_info(offers);")
        offers_cols = {row[1] for row in cur.fetchall()}
    except:
        offers_cols = set()
    
    try:
        cur = conn.execute("PRAGMA table_info(sent);")
        sent_cols = {row[1] for row in cur.fetchall()}
    except:
        sent_cols = set()
    
    # Добавляем колонки для времени в пути и защиты от дубликатов
    if "travel_time" not in offers_cols:
        if offers_cols:
            logging.warning("⟲ добавляем колонки content_hash, source и travel_time в таблицу offers")
            try:
                conn.execute("ALTER TABLE offers ADD COLUMN content_hash TEXT;")
                conn.execute("ALTER TABLE offers ADD COLUMN source TEXT;")
                conn.execute("ALTER TABLE offers ADD COLUMN travel_time TEXT;")
            except sqlite3.OperationalError:
                pass
        else:
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
                    source TEXT,
                    travel_time TEXT
                );
            """)
    
    conn.executescript("""
        CREATE INDEX IF NOT EXISTS idx_content_hash ON offers(content_hash);
        CREATE INDEX IF NOT EXISTS idx_price_rooms_area ON offers(price, rooms, area);
        CREATE INDEX IF NOT EXISTS idx_source_date ON offers(source, date);
    """)
    
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
    
    cur.execute("DELETE FROM offers WHERE date < ?", (cutoff_date,))
    deleted_offers = cur.rowcount
    
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
                    "disable_web_page_preview": False
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
    """Форматируем сообщение с добавлением времени в пути."""
    price = f"{offer['price']:,}".replace(",", " ")
    
    message = (
        f"{offer['url']}\n"
        f"{price} ₽ · {offer['rooms']}-к, {offer['area']} м²\n"
        f"{offer['address']}"
    )
    
    # Добавляем время в пути, если есть
    if offer.get('travel_time'):
        message += f"\n🚇 До места: {offer['travel_time']}"
    
    return message

def process_offer(offer: dict, conn: sqlite3.Connection) -> None:
    """Улучшенная обработка объявления с расчетом времени в пути."""
    if not accept_offer(offer):
        return
    
    offer["url"] = canon(offer["url"])
    url = offer["url"]
    content_hash = create_content_hash(offer)
    source = 'cian' if 'cian.ru' in url else 'yandex'
    
    # Получаем время в пути с отладкой
    logging.info("Рассчитываем время в пути для адреса: %s", offer['address'])
    travel_time = get_travel_time(offer['address'], DESTINATION_ADDRESS)
    offer['travel_time'] = travel_time
    
    if travel_time:
        logging.info("Время в пути рассчитано: %s", travel_time)
    else:
        logging.warning("Не удалось рассчитать время в пути для: %s", offer['address'])
    
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
        logging.info("Дубликат обнаружен (ID: %s), пропускаем: %s", existing[0], url)
        return
    
    # Сохраняем новое объявление
    try:
        cur.execute("""
            INSERT INTO offers
            (offer_id, url, content_hash, price, address, area, rooms, date, source, travel_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (offer['offer_id'], url, content_hash, offer['price'], 
              offer['address'], offer['area'], offer['rooms'], offer['date'], source, travel_time))
        
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
        travel_info = f" (время в пути: {travel_time})" if travel_time else ""
        logging.info("Новое объявление добавлено и отправлено в %s чатов: %s%s", 
                    new_chats, url, travel_info)
        
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
    try:
        area = float(item["totalArea"])
    except (ValueError, TypeError):
        area = 0.0
    
    return {
        "url": item["fullUrl"],
        "offer_id": item["id"],
        "date": datetime.fromtimestamp(item["addedTimestamp"]).strftime("%Y-%m-%d %H:%M:%S"),
        "price": item["bargainTerms"]["priceRur"],
        "address": item["geo"]["userInput"],
        "area": area,
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
    
    try:
        area = float(item["area"]["value"])
    except (ValueError, TypeError, KeyError):
        area = 0.0
    
    try:
        rooms = int(item["roomsTotalKey"])
    except (ValueError, TypeError):
        rooms = 1
    
    return {
        "url": item["shareUrl"],
        "offer_id": item["offerId"],
        "date": date_raw.replace("T", " ").replace("Z", ""),
        "price": item["price"]["value"],
        "address": item["location"]["address"],
        "area": area,
        "rooms": rooms,
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
    logging.info("Целевой адрес: %s", DESTINATION_ADDRESS)
    
    with db_conn() as conn:
        cleanup_old_offers(conn)
        
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM offers")
        offers_before = cur.fetchone()[0]
        
        logging.info("Парсинг Циан...")
        parse_cian(conn)
        
        logging.info("Парсинг Яндекс...")
        parse_yandex(conn)
        
        cur.execute("SELECT COUNT(*) FROM offers")
        offers_after = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(DISTINCT url) FROM sent")
        sent_offers = cur.fetchone()[0]
        
        new_offers = offers_after - offers_before
        
        logging.info("Статистика: новых объявлений: %s, всего: %s, отправлено: %s", 
                    new_offers, offers_after, sent_offers)

if __name__ == "__main__":
    main()
