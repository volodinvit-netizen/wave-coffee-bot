import os
import re
import requests
from datetime import datetime, timedelta, timezone

from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy import text


# =========================
# НАСТРОЙКИ
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN")
POSTER_TOKEN = os.getenv("POSTER_TOKEN")
POSTER_DOMAIN = os.getenv("POSTER_DOMAIN")
DATABASE_URL = os.getenv("DATABASE_URL")

if not BOT_TOKEN:
    raise RuntimeError("Не задан BOT_TOKEN в Environment")
if not POSTER_TOKEN:
    raise RuntimeError("Не задан POSTER_TOKEN в Environment")
if not POSTER_DOMAIN:
    raise RuntimeError("Не задан POSTER_DOMAIN в Environment")
if not DATABASE_URL:
    raise RuntimeError("Не задан DATABASE_URL в Environment")

BASE_URL = f"https://{POSTER_DOMAIN}/api"

DAILY_LIMIT = 2
RECEIPT_TTL_MINUTES = 10


# =========================
# БАЗА
# =========================
engine = create_async_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False)


async def create_or_update_tables():
    """
    Создаёт таблицы и добавляет недостающие колонки.
    Это избавляет от ручных "миграций" на раннем этапе.
    """
    async with engine.begin() as conn:
        await conn.execute(text("""
        CREATE TABLE IF NOT EXISTS users (
            id BIGSERIAL PRIMARY KEY,
            telegram_id BIGINT UNIQUE,
            username TEXT,
            balance BIGINT DEFAULT 0
        );
        """))

        await conn.execute(text("""
        CREATE TABLE IF NOT EXISTS receipts (
            id BIGSERIAL PRIMARY KEY,
            transaction_id TEXT UNIQUE,
            telegram_id BIGINT,
            amount BIGINT,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            poster_time TIMESTAMPTZ
        );
        """))

        # Добавляем новые поля (если их нет)
        await conn.execute(text("ALTER TABLE users ADD COLUMN IF NOT EXISTS referrer BIGINT;"))
        await conn.execute(text("ALTER TABLE receipts ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW();"))
        await conn.execute(text("ALTER TABLE receipts ADD COLUMN IF NOT EXISTS poster_time TIMESTAMPTZ;"))


async def ensure_user_exists(tg_id: int, username: str | None, referrer: int | None = None):
    async with SessionLocal() as session:
        await session.execute(text("""
            INSERT INTO users (telegram_id, username, balance, referrer)
            VALUES (:tg, :username, 0, :referrer)
            ON CONFLICT (telegram_id) DO NOTHING
        """), {"tg": tg_id, "username": username, "referrer": referrer})

        # если referrer передан и у пользователя его ещё нет — заполним один раз
        if referrer is not None:
            await session.execute(text("""
                UPDATE users
                SET referrer = COALESCE(referrer, :referrer)
                WHERE telegram_id = :tg
            """), {"tg": tg_id, "referrer": referrer})

        await session.commit()


# =========================
# POSTER
# =========================
def get_transaction(transaction_id: str):
    url = f"{BASE_URL}/dash.getTransaction"
    params = {"token": POSTER_TOKEN, "transaction_id": transaction_id}
    r = requests.get(url, params=params, timeout=15)

    # лог полезен при проблемах
    print("POSTER URL:", r.url)
    print("POSTER STATUS:", r.status_code)
    print("POSTER RAW (first 300 chars):", r.text[:300])

    try:
        return r.json()
    except Exception:
        return {"error": {"message": "Poster вернул не JSON"}, "status": r.status_code}


def extract_poster_total_tenge(transaction: dict) -> int:
    """
    Надёжно получаем сумму чека в тенге.
    Poster может отдавать сумму:
    - в total / sum / total_sum
    - либо уже в тенге (например 3790)
    - либо *100 (например 379000)
    """
    raw = transaction.get("total") or transaction.get("sum") or transaction.get("total_sum") or 0
    try:
        raw = float(raw)
    except Exception:
        raw = 0.0

    # эвристика:
    # если число слишком большое — это почти наверняка *100
    if raw >= 100000:  # 1000.00 ₸ => 100000
        return int(round(raw / 100))

    return int(round(raw))


def extract_poster_time(transaction: dict) -> datetime | None:
    """
    Best effort: пытаемся вытащить время чека из Poster.
    Если не получится — вернём None, и правило 10 минут не применим.
    """
    for key in ("date_close", "date", "created_at", "time", "closed_at"):
        v = transaction.get(key)
        if not v:
            continue

        # epoch seconds
        if isinstance(v, (int, float)) and v > 1000000000:
            try:
                return datetime.fromtimestamp(float(v), tz=timezone.utc)
            except Exception:
                pass

        # строка
        if isinstance(v, str):
            s = v.strip()
            fmts = [
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%dT%H:%M:%S.%f",
                "%d.%m.%Y %H:%M:%S",
                "%d.%m.%Y %H:%M",
            ]
            for fmt in fmts:
                try:
                    dt = datetime.strptime(s, fmt)
                    return dt.replace(tzinfo=timezone.utc)
                except Exception:
                    continue

    return None


# =========================
# PARSE
# =========================
def parse_receipt_number(text_msg: str) -> str | None:
    m = re.search(r"\d{4,}", text_msg or "")
    return m.group(0) if m else None


def parse_amount_tenge(text_msg: str) -> int | None:
    """
    Понимает: 3790 / 3 790 / 3,790 / 3.790 / 3790₸
    Возвращает целые тенге.
    """
    if not text_msg:
        return None

    s = text_msg.strip()
    m = re.search(r"(\d[\d\s.,]*)", s)
    if not m:
        return None

    num = m.group(1).replace(" ", "")

    if "." in num and "," in num:
        num = num.replace(".", "").replace(",", "")
        try:
            return int(float(num))
        except Exception:
            return None

    if "," in num and "." not in num:
        parts = num.split(",")
        if len(parts) == 2 and 1 <= len(parts[1]) <= 2:
            num = num.replace(",", ".")
        else:
            num = num.replace(",", "")

    if "." in num:
        parts = num.split(".")
        if len(parts) == 2 and 1 <= len(parts[1]) <= 2:
            try:
                return int(float(num))
            except Exception:
                return None
        else:
            num = num.replace(".", "")

    try:
        return int(float(num))
    except Exception:
        return None


# =========================
# КОМАНДЫ
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tg_id = update.effective_user.id
    username = update.effective_user.username

    # если когда-то будем использовать реф-ссылки: /start 123456
    referrer = None
    if context.args:
        try:
            referrer = int(context.args[0])
        except Exception:
            referrer = None

    await ensure_user_exists(tg_id, username, referrer=referrer)

    context.user_data.pop("pending_receipt_id", None)
    context.user_data.pop("pending_total", None)
    context.user_data.pop("pending_poster_time", None)

    await update.message.reply_text(
        "☕ Добро пожаловать в Wave Coffee Rewards!\n\n"
        "Шаг 1: отправьте номер чека Poster.\n"
        "Шаг 2: бот попросит сумму — вы её введёте, и мы начислим баллы."
    )


async def balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tg_id = update.effective_user.id
    await ensure_user_exists(tg_id, update.effective_user.username)

    async with SessionLocal() as session:
        r = await session.execute(
            text("SELECT balance FROM users WHERE telegram_id = :tg"),
            {"tg": tg_id}
        )
        bal = r.scalar() or 0

    await update.message.reply_text(f"Ваш баланс: {bal} баллов")


# =========================
# ОСНОВНАЯ ЛОГИКА (номер -> сумма)
# =========================
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tg_id = update.effective_user.id
    username = update.effective_user.username
    await ensure_user_exists(tg_id, username)

    text_msg = (update.message.text or "").strip()

    # ---- 2 шаг: ждём сумму
    if context.user_data.get("pending_receipt_id") and context.user_data.get("pending_total") is not None:
        amount = parse_amount_tenge(text_msg)
        if amount is None:
            await update.message.reply_text("Не понял сумму. Введите просто число, например: 3790")
            return

        expected_total = int(context.user_data["pending_total"])
        receipt_id = str(context.user_data["pending_receipt_id"])
        poster_time = context.user_data.get("pending_poster_time")

        if amount != expected_total:
            await update.message.reply_text("⚠️ Сумма не совпала. Введите сумму с чека ещё раз (числом).")
            return

        # проверка срока жизни чека (если Poster дал время)
        if isinstance(poster_time, datetime):
            now_utc = datetime.now(timezone.utc)
            if now_utc - poster_time > timedelta(minutes=RECEIPT_TTL_MINUTES):
                context.user_data.clear()
                await update.message.reply_text(
                    f"⚠️ Чек уже неактуален. Можно активировать только в течение {RECEIPT_TTL_MINUTES} минут."
                )
                return

        cashback = int(expected_total * 0.05)

        async with SessionLocal() as session:
            # дубль чека
            check = await session.execute(
                text("SELECT id FROM receipts WHERE transaction_id = :tid"),
                {"tid": receipt_id}
            )
            if check.first():
                context.user_data.clear()
                await update.message.reply_text("⚠️ Этот чек уже активирован.")
                return

            # лимит чеков в день
            cnt = await session.execute(text("""
                SELECT COUNT(*) FROM receipts
                WHERE telegram_id = :tg
                  AND created_at >= date_trunc('day', NOW())
            """), {"tg": tg_id})
            if (cnt.scalar() or 0) >= DAILY_LIMIT:
                context.user_data.clear()
                await update.message.reply_text(f"⚠️ Лимит: {DAILY_LIMIT} чека(ов) в день. Попробуйте завтра.")
                return

            # сохранить чек
            await session.execute(text("""
                INSERT INTO receipts (transaction_id, telegram_id, amount, poster_time)
                VALUES (:tid, :tg, :amount, :poster_time)
            """), {
                "tid": receipt_id,
                "tg": tg_id,
                "amount": expected_total,
                "poster_time": poster_time,
            })

            # начислить баланс
            await session.execute(text("""
                UPDATE users
                SET balance = balance + :cashback
                WHERE telegram_id = :tg
            """), {"cashback": cashback, "tg": tg_id})

            await session.commit()

            # новый баланс
            r = await session.execute(
                text("SELECT balance FROM users WHERE telegram_id = :tg"),
                {"tg": tg_id}
            )
            new_balance = r.scalar() or 0

        context.user_data.clear()

        await update.message.reply_text(
            "✅ Готово!\n"
            f"Чек: {receipt_id}\n"
            f"Сумма: {expected_total} ₸\n"
            f"Начислено 5%: +{cashback} баллов\n"
            f"Ваш баланс: {new_balance} баллов"
        )
        return

    # ---- 1 шаг: ждём номер чека
    receipt_id = parse_receipt_number(text_msg)
    if not receipt_id:
        await update.message.reply_text("Введите номер чека Poster, например: 426374")
        return

    data = get_transaction(receipt_id)

    # ошибки Poster
    if isinstance(data, dict) and "error" in data and data["error"]:
        err = data["error"]
        msg = err.get("message") if isinstance(err, dict) else str(err)
        await update.message.reply_text(f"Ошибка Poster: {msg}")
        return

    if not isinstance(data, dict) or "response" not in data:
        await update.message.reply_text("Чек не найден в Poster")
        return

    resp = data["response"]
    transaction = resp[0] if isinstance(resp, list) and resp else resp

    total = extract_poster_total_tenge(transaction)
    poster_time = extract_poster_time(transaction)

    # Отладочный лог (можно оставить, не мешает)
    print("EXPECTED TOTAL (tenge):", total, "transaction_id:", receipt_id)

    # до запроса суммы — проверим дубль и лимит
    async with SessionLocal() as session:
        check = await session.execute(
            text("SELECT id FROM receipts WHERE transaction_id = :tid"),
            {"tid": receipt_id}
        )
        if check.first():
            await update.message.reply_text("⚠️ Этот чек уже активирован.")
            return

        cnt = await session.execute(text("""
            SELECT COUNT(*) FROM receipts
            WHERE telegram_id = :tg
              AND created_at >= date_trunc('day', NOW())
        """), {"tg": tg_id})
        if (cnt.scalar() or 0) >= DAILY_LIMIT:
            await update.message.reply_text(f"⚠️ Лимит: {DAILY_LIMIT} чека(ов) в день. Попробуйте завтра.")
            return

    context.user_data["pending_receipt_id"] = receipt_id
    context.user_data["pending_total"] = total
    context.user_data["pending_poster_time"] = poster_time

    await update.message.reply_text("✅ Чек найден.\nВведите сумму чека (числом).")


async def on_startup(app):
    await create_or_update_tables()


def main():
    app = Application.builder().token(BOT_TOKEN).post_init(on_startup).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("balance", balance))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    print("Bot started...")
    app.run_polling()


if __name__ == "__main__":
    main()
