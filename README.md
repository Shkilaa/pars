# Realty Parser Telegram Bot

Парсер объявлений об аренде **однокомнатных квартир в Москве** с сайтов **Циан** и **Яндекс.Недвижимость** с автоматической рассылкой уведомлений в Telegram-чаты[1].

## Возможности
- Парсинг объявлений с фильтрами: *1-комнатные, цена ≤ 50 000 ₽*[1].
- Сохранение новых объявлений в локальную базу SQLite (`offers.db`)[1].
- Рассылка уведомлений о каждом новом объекте в несколько Telegram-чатов.
- Учёт лимитов Telegram API и автоматический повтор при 429 / сетевых ошибках.
- Итоговая сводка по количеству обработанных и новых объявлений
  (отправляется только при наличии новинок).

## Требования
- Python 3.8+
- Библиотека `requests`[1]

## Установка
```
pip install requests
```

## Настройка
1. Откройте `realty_fullfeed.py`.
2. Укажите:
   - `TG_BOT_TOKEN` — токен Telegram-бота.
   - `CHAT_IDS` — список chat-id, куда слать объявления (через запятую)[1].
3. При первом запуске автоматически создаётся база `offers.db`.

## Запуск
```
python realty_fullfeed.py
```

## Как работает
1. Скрипт обращается к API Циан и Яндекс.Недвижимость.
2. Фильтрует выдачу по количеству комнат и цене.
3. Новые объявления сохраняются в SQLite и отправляются в Telegram.
4. Для каждого чата ведётся история отправок, поэтому дубликаты не
   рассылаются.
5. В конце формируется сводка с числом обработанных / доставленных
   объявлений[1].

## Размещение (бесплатные варианты)
| Платформа      | Особенности | Что сделать |
|----------------|-------------|-------------|
| **Replit** + UptimeRobot | Постоянная ФС, легко настроить | создать Repl, настроить secrets, добавить `keep_alive.py`, пинговать через UptimeRobot |
| **GitHub Actions** | 2 000 мин/мес бесплатно | workflow c cron, артефакт для `offers.db` |
| **Railway**    | 500 ч CPU / мес, постоянная ФС | задать cron-job `python realty_fullfeed.py` |

## Лицензия
MIT
```
