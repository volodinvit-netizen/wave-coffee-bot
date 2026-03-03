import os
import re
import requests

from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy import text


# -------------------------
# Настройки из Render -> Environment
# -------------------------
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


# -------------------------
# Подключение к базе
# -------------------------
engine = create_async_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False)


async def create_tables():
    """Создаёт таблицы в базе, если их ещё нет."""
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
            amount BIGINT
        );
        """))


def get_transaction(transaction_id: str):
    """Запрашивает чек из Poster."""
    url = f"{BASE_URL}/dash.getTransaction"
    params = {"token": POSTER_TOKEN, "transaction_id": transaction_id}

    r = requests.get(url, params=params, timeout=15)

    # Логи в Render (если нужно для отладки)
    print("POSTER URL:", r.url)
    print("POSTER STATUS:", r.status_code)
    print("POSTER RAW (first 300 chars):", r.text[:300])

    try:
        return r.json()
    except Exception:
        return {"error": {"message": "Poster вернул не JSON"}, "status": r.status_code}


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "☕ Добро пожаловать в Wave Coffee Rewards!\n\n"
        "Введите номер: Чек Poster № (например 426374), чтобы получить баллы."
    )


async def handle_check(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text_msg = (update.message.text or "").strip()

    # достаём номер чека из сообщения
    m = re.search(r"\d{4,}", text_msg)
    if not m:
        await update.message.reply_text("Введите номер чека Poster, например: 426374")
        return

    receipt_id = m.group(0)

    data = get_transaction(receipt_id)

    # обработка ошибок Poster
    if isinstance(data, dict) and "error" in data and data["error"]:
        err = data["error"]
        msg = err.get("message") if isinstance(err, dict) else str(err)
        await update.message.reply_text(f"Poster ошибка: {msg}")
        return

    if not isinstance(data, dict) or "response" not in data:
        await update.message.reply_text("Чек не найден в Poster")
        return

    resp = data["response"]
    transaction = resp[0] if isinstance(resp, list) and resp else resp

    raw_total = transaction.get("total") or transaction.get("sum") or transaction.get("total_sum") or 0
    try:
        raw_total = float(raw_total)
    except Exception:
        raw_total = 0.0

    total = raw_total / 100
    cashback = int(total * 0.05)

    tg_id = update.effective_user.id
    username = update.effective_user.username

    async with SessionLocal() as session:
        # 1) проверяем, активировали ли уже этот чек
        check = await session.execute(
            text("SELECT id FROM receipts WHERE transaction_id = :tid"),
            {"tid": receipt_id}
        )
        if check.first():
            await update.message.reply_text("⚠️ Этот чек уже активирован.")
            return

        # 2) создаём пользователя, если его нет
        await session.execute(
            text("""
            INSERT INTO users (telegram_id, username)
            VALUES (:tg, :username)
            ON CONFLICT (telegram_id) DO NOTHING
            """),
            {"tg": tg_id, "username": username}
        )

        # 3) сохраняем чек
        await session.execute(
            text("""
            INSERT INTO receipts (transaction_id, telegram_id, amount)
            VALUES (:tid, :tg, :amount)
            """),
            {"tid": receipt_id, "tg": tg_id, "amount": int(total)}
        )

        # 4) начисляем баланс
        await session.execute(
            text("""
            UPDATE users
            SET balance = balance + :cashback
            WHERE telegram_id = :tg
            """),
            {"cashback": cashback, "tg": tg_id}
        )

        await session.commit()

        # 5) получаем новый баланс
        result = await session.execute(
            text("SELECT balance FROM users WHERE telegram_id = :tg"),
            {"tg": tg_id}
        )
        balance = result.scalar()

    await update.message.reply_text(
        f"✅ Чек найден!\n"
        f"Номер: {receipt_id}\n"
        f"Сумма: {int(total)} ₸\n"
        f"Начислено 5%: +{cashback} баллов\n"
        f"Ваш баланс: {balance} баллов"
    )


async def on_startup(app):
    # создаём таблицы при старте бота (правильно, без asyncio.run)
    await create_tables()


def main():
    app = Application.builder().token(BOT_TOKEN).post_init(on_startup).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_check))

    print("Bot started...")
    app.run_polling()


if __name__ == "__main__":
    main()
