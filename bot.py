import os
import re
import requests
from datetime import datetime, timedelta, timezone

from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy import text


# =========================
# НАСТРОЙКИ (Render -> Environment)
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN")
POSTER_TOKEN = os.getenv("POSTER_TOKEN")
POSTER_DOMAIN = os.getenv("POSTER_DOMAIN")
DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_TG_ID = int(os.getenv("ADMIN_TG_ID", "0"))

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

# Разрешаем небольшую разницу, чтобы не было “почему не совпало, хотя я ввёл правильно”
AMOUNT_TOLERANCE_TENGE = 2  # можно поставить 1, если хочешь строже


# =========================
# БАЗА
# =========================
engine = create_async_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False)


async def create_or_update_tables():
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
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        """))

        await conn.execute(text("ALTER TABLE users ADD COLUMN IF NOT EXISTS referrer BIGINT;"))
        await conn.execute(text("ALTER TABLE receipts ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW();"))
        await conn.execute(text("ALTER TABLE receipts ADD COLUMN IF NOT EXISTS poster_time TIMESTAMPTZ;"))


async def ensure_user_exists(tg_id: int, username: str | None):
    async with SessionLocal() as session:
        await session.execute(text("""
        INSERT INTO users (telegram_id, username)
        VALUES (:tg, :username)
        ON CONFLICT (telegram_id) DO NOTHING
        """), {"tg": tg_id, "username": username})
        await session.commit()


# =========================
# POSTER
# =========================
def get_transaction(transaction_id: str):
    url = f"{BASE_URL}/dash.getTransaction"
    params = {"token": POSTER_TOKEN, "transaction_id": transaction_id}
    r = requests.get(url, params=params, timeout=15)

    try:
        return r.json()
    except Exception:
        return {"error": "poster_non_json"}


def extract_total_tenge(transaction: dict) -> int:
    raw = transaction.get("total") or transaction.get("sum") or transaction.get("total_sum") or 0
    try:
        raw = float(raw)
    except Exception:
        raw = 0.0

    # Если очень большое — почти наверняка *100
    if raw >= 100000:
        return int(round(raw / 100))

    return int(round(raw))


def extract_poster_time(transaction: dict) -> datetime | None:
    keys = ["date_close", "date", "created_at", "closed_at", "time"]
    for k in keys:
        v = transaction.get(k)
        if not v:
            continue

        if isinstance(v, (int, float)) and v > 1000000000:
            try:
                return datetime.fromtimestamp(float(v), tz=timezone.utc)
            except Exception:
                pass

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


def is_receipt_too_old(poster_time: datetime | None) -> bool:
    if poster_time is None:
        return False
    now_utc = datetime.now(timezone.utc)
    return (now_utc - poster_time) > timedelta(minutes=RECEIPT_TTL_MINUTES)


# =========================
# PARSE
# =========================
def parse_receipt(text_msg: str) -> str | None:
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

    await ensure_user_exists(tg_id, username)
    context.user_data.clear()

    await update.message.reply_text(
        "☕ Добро пожаловать в Wave Coffee Rewards!\n\n"
        "Шаг 1: отправьте номер чека Poster.\n"
        "Шаг 2: бот попросит сумму — вы её введёте."
    )


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await update.message.reply_text("Ок. Сбросил. Введите номер чека Poster.")


async def balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tg_id = update.effective_user.id
    await ensure_user_exists(tg_id, update.effective_user.username)

    async with SessionLocal() as session:
        r = await session.execute(
            text("SELECT balance FROM users WHERE telegram_id=:tg"),
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

    # ---------- 2 шаг: ждём сумму
    if context.user_data.get("wait_sum"):
        amount = parse_amount_tenge(text_msg)
        if amount is None:
            await update.message.reply_text("Не понял сумму. Введите просто число, например: 3790")
            return

        expected = int(context.user_data.get("poster_sum", 0))
        receipt_id = str(context.user_data.get("receipt", ""))

        if not receipt_id or expected <= 0:
            context.user_data.clear()
            await update.message.reply_text("Что-то пошло не так. Начните заново: отправьте номер чека.")
            return

        # сравнение с допуском ±2 тенге
        if abs(amount - expected) > AMOUNT_TOLERANCE_TENGE:
            await update.message.reply_text(
                "⚠️ Сумма не совпала.\n"
                "Введите сумму с чека ещё раз (числом).\n"
                "Если запутались — напишите /cancel"
            )
            return

        poster_time = context.user_data.get("poster_time")  # datetime | None

        # Проверка 10 минут — НО админ обходит
        if tg_id != ADMIN_TG_ID and is_receipt_too_old(poster_time):
            context.user_data.clear()
            await update.message.reply_text(
                f"⚠️ Чек уже неактуален. Можно активировать только в течение {RECEIPT_TTL_MINUTES} минут.\n"
                "Начните заново: отправьте номер чека."
            )
            return

        cashback = int(expected * 0.05)

        async with SessionLocal() as session:
            # дубль чека
            check = await session.execute(
                text("SELECT id FROM receipts WHERE transaction_id=:tid"),
                {"tid": receipt_id}
            )
            if check.first():
                context.user_data.clear()
                await update.message.reply_text("⚠️ Этот чек уже активирован.")
                return

            # лимит чеков в день — админ обходит
            if tg_id != ADMIN_TG_ID:
                today = await session.execute(text("""
                SELECT COUNT(*) FROM receipts
                WHERE telegram_id=:tg
                  AND created_at >= date_trunc('day', NOW())
                """), {"tg": tg_id})

                if (today.scalar() or 0) >= DAILY_LIMIT:
                    context.user_data.clear()
                    await update.message.reply_text(f"⚠️ Лимит: {DAILY_LIMIT} чека(ов) в день. Попробуйте завтра.")
                    return

            await session.execute(text("""
            INSERT INTO receipts (transaction_id, telegram_id, amount, poster_time)
            VALUES (:tid, :tg, :amount, :poster_time)
            """), {
                "tid": receipt_id,
                "tg": tg_id,
                "amount": expected,
                "poster_time": poster_time
            })

            await session.execute(text("""
            UPDATE users
            SET balance = balance + :b
            WHERE telegram_id=:tg
            """), {"b": cashback, "tg": tg_id})

            await session.commit()

            r = await session.execute(
                text("SELECT balance FROM users WHERE telegram_id=:tg"),
                {"tg": tg_id}
            )
            new_balance = r.scalar() or 0

        context.user_data.clear()

        await update.message.reply_text(
            "✅ Готово!\n"
            f"Чек: {receipt_id}\n"
            f"Сумма: {expected} ₸\n"
            f"Начислено 5%: +{cashback} баллов\n"
            f"Ваш баланс: {new_balance} баллов"
        )
        return

    # ---------- 1 шаг: ждём номер чека
    receipt = parse_receipt(text_msg)
    if not receipt:
        await update.message.reply_text("Введите номер чека Poster, например: 426374")
        return

    data = get_transaction(receipt)

    if not isinstance(data, dict) or "response" not in data:
        await update.message.reply_text("Чек не найден в Poster")
        return

    resp = data["response"]
    transaction = resp[0] if isinstance(resp, list) and resp else resp

    total = extract_total_tenge(transaction)
    poster_time = extract_poster_time(transaction)

    print("EXPECTED TOTAL:", total, "POSTER_TIME:", poster_time, "RECEIPT:", receipt)

    async with SessionLocal() as session:
        check = await session.execute(
            text("SELECT id FROM receipts WHERE transaction_id=:tid"),
            {"tid": receipt}
        )
        if check.first():
            await update.message.reply_text("⚠️ Этот чек уже активирован.")
            return

        if tg_id != ADMIN_TG_ID:
            today = await session.execute(text("""
            SELECT COUNT(*) FROM receipts
            WHERE telegram_id=:tg
              AND created_at >= date_trunc('day', NOW())
            """), {"tg": tg_id})

            if (today.scalar() or 0) >= DAILY_LIMIT:
                await update.message.reply_text(f"⚠️ Лимит: {DAILY_LIMIT} чека(ов) в день. Попробуйте завтра.")
                return

    context.user_data["wait_sum"] = True
    context.user_data["poster_sum"] = total
    context.user_data["receipt"] = receipt
    context.user_data["poster_time"] = poster_time

    await update.message.reply_text("✅ Чек найден.\nВведите сумму чека (числом).")


async def on_startup(app):
    await create_or_update_tables()


def main():
    app = Application.builder().token(BOT_TOKEN).post_init(on_startup).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("cancel", cancel))
    app.add_handler(CommandHandler("balance", balance))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    print("Bot started...")
    app.run_polling()


if __name__ == "__main__":
    main()
