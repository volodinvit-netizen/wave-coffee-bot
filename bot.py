import os
import re
import requests
from datetime import datetime, timedelta, timezone

from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy import text


BOT_TOKEN = os.getenv("BOT_TOKEN")
POSTER_TOKEN = os.getenv("POSTER_TOKEN")
POSTER_DOMAIN = os.getenv("POSTER_DOMAIN")
DATABASE_URL = os.getenv("DATABASE_URL")

ADMIN_TG_ID = int(os.getenv("ADMIN_TG_ID", "0"))

BASE_URL = f"https://{POSTER_DOMAIN}/api"

DAILY_LIMIT = 2
RECEIPT_TTL_MINUTES = 10


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

        await conn.execute(text(
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS referrer BIGINT;"
        ))


async def ensure_user_exists(tg_id, username):

    async with SessionLocal() as session:

        await session.execute(text("""
        INSERT INTO users (telegram_id, username)
        VALUES (:tg, :username)
        ON CONFLICT (telegram_id) DO NOTHING
        """), {"tg": tg_id, "username": username})

        await session.commit()


def get_transaction(transaction_id):

    url = f"{BASE_URL}/dash.getTransaction"

    params = {
        "token": POSTER_TOKEN,
        "transaction_id": transaction_id
    }

    r = requests.get(url, params=params, timeout=15)

    try:
        return r.json()
    except Exception:
        return {"error": "poster_non_json"}


def extract_total(transaction):

    raw = (
        transaction.get("total")
        or transaction.get("sum")
        or transaction.get("total_sum")
        or 0
    )

    try:
        raw = float(raw)
    except:
        raw = 0

    if raw >= 100000:
        return int(raw / 100)

    return int(raw)


def parse_receipt(text):

    m = re.search(r"\d{4,}", text)

    if m:
        return m.group(0)

    return None


def parse_amount(text):

    m = re.search(r"\d+", text)

    if not m:
        return None

    return int(m.group(0))


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


async def balance(update: Update, context: ContextTypes.DEFAULT_TYPE):

    tg_id = update.effective_user.id

    async with SessionLocal() as session:

        r = await session.execute(text("""
        SELECT balance FROM users
        WHERE telegram_id=:tg
        """), {"tg": tg_id})

        bal = r.scalar() or 0

    await update.message.reply_text(f"Ваш баланс: {bal} баллов")


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):

    tg_id = update.effective_user.id
    username = update.effective_user.username

    await ensure_user_exists(tg_id, username)

    text_msg = update.message.text.strip()

    if context.user_data.get("wait_sum"):

        amount = parse_amount(text_msg)

        if not amount:

            await update.message.reply_text("Введите сумму чека числом")

            return

        expected = context.user_data["poster_sum"]
        receipt_id = context.user_data["receipt"]

        if amount != expected:

            await update.message.reply_text("Сумма не совпала. Попробуйте снова.")

            return

        cashback = int(expected * 0.05)

        async with SessionLocal() as session:

            check = await session.execute(text("""
            SELECT id FROM receipts
            WHERE transaction_id=:tid
            """), {"tid": receipt_id})

            if check.first():

                await update.message.reply_text("Этот чек уже активирован")

                return

            if tg_id != ADMIN_TG_ID:

                today = await session.execute(text("""
                SELECT COUNT(*)
                FROM receipts
                WHERE telegram_id=:tg
                AND created_at >= CURRENT_DATE
                """), {"tg": tg_id})

                if today.scalar() >= DAILY_LIMIT:

                    await update.message.reply_text(
                        "⚠️ Лимит 2 чека в день"
                    )

                    return

            await session.execute(text("""
            INSERT INTO receipts (transaction_id, telegram_id, amount)
            VALUES (:tid,:tg,:amount)
            """), {
                "tid": receipt_id,
                "tg": tg_id,
                "amount": expected
            })

            await session.execute(text("""
            UPDATE users
            SET balance = balance + :b
            WHERE telegram_id=:tg
            """), {"b": cashback, "tg": tg_id})

            await session.commit()

            r = await session.execute(text("""
            SELECT balance FROM users
            WHERE telegram_id=:tg
            """), {"tg": tg_id})

            balance = r.scalar()

        context.user_data.clear()

        await update.message.reply_text(
            f"Чек принят\n"
            f"Начислено {cashback} баллов\n"
            f"Баланс {balance}"
        )

        return

    receipt = parse_receipt(text_msg)

    if not receipt:

        await update.message.reply_text(
            "Введите номер чека Poster"
        )

        return

    data = get_transaction(receipt)

    if "response" not in data:

        await update.message.reply_text(
            "Чек не найден"
        )

        return

    resp = data["response"]

    transaction = resp[0] if isinstance(resp, list) else resp

    total = extract_total(transaction)

    print("EXPECTED:", total)

    context.user_data["wait_sum"] = True
    context.user_data["poster_sum"] = total
    context.user_data["receipt"] = receipt

    await update.message.reply_text(
        "Введите сумму чека"
    )


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
