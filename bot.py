import os
import re
import requests
from datetime import datetime, timedelta

from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy import text


BOT_TOKEN = os.getenv("BOT_TOKEN")
POSTER_TOKEN = os.getenv("POSTER_TOKEN")
POSTER_DOMAIN = os.getenv("POSTER_DOMAIN")
DATABASE_URL = os.getenv("DATABASE_URL")

BASE_URL = f"https://{POSTER_DOMAIN}/api"


engine = create_async_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False)


async def create_tables():
    async with engine.begin() as conn:

        await conn.execute(text("""
        CREATE TABLE IF NOT EXISTS users (
            id BIGSERIAL PRIMARY KEY,
            telegram_id BIGINT UNIQUE,
            username TEXT,
            balance BIGINT DEFAULT 0,
            referrer BIGINT
        );
        """))

        await conn.execute(text("""
        CREATE TABLE IF NOT EXISTS receipts (
            id BIGSERIAL PRIMARY KEY,
            transaction_id TEXT UNIQUE,
            telegram_id BIGINT,
            amount BIGINT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """))


def get_transaction(transaction_id: str):
    url = f"{BASE_URL}/dash.getTransaction"
    params = {"token": POSTER_TOKEN, "transaction_id": transaction_id}

    r = requests.get(url, params=params, timeout=15)

    try:
        return r.json()
    except Exception:
        return {"error": "poster_non_json"}


def parse_receipt_number(text_msg: str):
    m = re.search(r"\d{4,}", text_msg)
    return m.group(0) if m else None


def parse_amount(text_msg: str):
    m = re.search(r"\d+", text_msg)
    return int(m.group(0)) if m else None


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):

    tg_id = update.effective_user.id
    username = update.effective_user.username

    ref = None
    if context.args:
        ref = context.args[0]

    async with SessionLocal() as session:

        await session.execute(text("""
        INSERT INTO users (telegram_id, username, referrer)
        VALUES (:tg, :username, :ref)
        ON CONFLICT (telegram_id) DO NOTHING
        """), {"tg": tg_id, "username": username, "ref": ref})

        await session.commit()

    await update.message.reply_text(
        "☕ Добро пожаловать в Wave Coffee Rewards!\n\n"
        "Введите номер чека Poster."
    )


async def balance(update: Update, context: ContextTypes.DEFAULT_TYPE):

    tg_id = update.effective_user.id

    async with SessionLocal() as session:

        result = await session.execute(
            text("SELECT balance FROM users WHERE telegram_id = :tg"),
            {"tg": tg_id}
        )

        bal = result.scalar() or 0

    await update.message.reply_text(f"Ваш баланс: {bal} баллов")


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):

    text_msg = update.message.text.strip()

    if context.user_data.get("wait_sum"):

        amount = parse_amount(text_msg)

        if not amount:
            await update.message.reply_text("Введите сумму чека числом.")
            return

        receipt_id = context.user_data["receipt_id"]
        poster_sum = context.user_data["poster_sum"]

        if amount != poster_sum:
            await update.message.reply_text("Сумма не совпала. Попробуйте снова.")
            return

        cashback = int(amount * 0.05)
        tg_id = update.effective_user.id

        async with SessionLocal() as session:

            check = await session.execute(
                text("SELECT id FROM receipts WHERE transaction_id=:tid"),
                {"tid": receipt_id}
            )

            if check.first():
                await update.message.reply_text("Чек уже активирован.")
                return

            today_check = await session.execute(text("""
            SELECT COUNT(*) FROM receipts
            WHERE telegram_id=:tg
            AND created_at >= CURRENT_DATE
            """), {"tg": tg_id})

            if today_check.scalar() >= 2:
                await update.message.reply_text("Лимит 2 чека в день.")
                return

            await session.execute(text("""
            INSERT INTO receipts (transaction_id, telegram_id, amount)
            VALUES (:tid,:tg,:amount)
            """), {"tid": receipt_id, "tg": tg_id, "amount": amount})

            await session.execute(text("""
            UPDATE users SET balance = balance + :cashback
            WHERE telegram_id = :tg
            """), {"cashback": cashback, "tg": tg_id})

            ref = await session.execute(
                text("SELECT referrer FROM users WHERE telegram_id=:tg"),
                {"tg": tg_id}
            )

            ref1 = ref.scalar()

            if ref1:

                r1 = int(amount * 0.03)
                await session.execute(text("""
                UPDATE users SET balance = balance + :b
                WHERE telegram_id = :tg
                """), {"b": r1, "tg": ref1})

                r2 = await session.execute(
                    text("SELECT referrer FROM users WHERE telegram_id=:tg"),
                    {"tg": ref1}
                )

                ref2 = r2.scalar()

                if ref2:

                    b2 = int(amount * 0.02)

                    await session.execute(text("""
                    UPDATE users SET balance = balance + :b
                    WHERE telegram_id = :tg
                    """), {"b": b2, "tg": ref2})

            await session.commit()

        context.user_data.clear()

        await update.message.reply_text(
            f"Чек принят.\n"
            f"Начислено {cashback} баллов."
        )

        return

    receipt_id = parse_receipt_number(text_msg)

    if not receipt_id:
        await update.message.reply_text("Введите номер чека.")
        return

    data = get_transaction(receipt_id)

    if "response" not in data:
        await update.message.reply_text("Чек не найден.")
        return

    transaction = data["response"][0] if isinstance(data["response"], list) else data["response"]

    raw_total = float(transaction.get("total") or 0)
    total = int(raw_total / 100)

    context.user_data["wait_sum"] = True
    context.user_data["receipt_id"] = receipt_id
    context.user_data["poster_sum"] = total

    await update.message.reply_text("Введите сумму чека.")


async def on_startup(app):
    await create_tables()


def main():

    app = Application.builder().token(BOT_TOKEN).post_init(on_startup).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("balance", balance))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    print("Bot started...")
    app.run_polling()


if __name__ == "__main__":
    main()
