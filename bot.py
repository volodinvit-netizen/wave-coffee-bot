import os
import requests
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy import text

BOT_TOKEN = os.getenv("BOT_TOKEN")
POSTER_TOKEN = os.getenv("POSTER_TOKEN")
POSTER_DOMAIN = os.getenv("POSTER_DOMAIN")

BASE_URL = f"https://{POSTER_DOMAIN}/api"
DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_async_engine(DATABASE_URL)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False)

def get_transaction(transaction_id: str):
    url = f"{BASE_URL}/dash.getTransaction"
    params = {"token": POSTER_TOKEN, "transaction_id": transaction_id}
    r = requests.get(url, params=params, timeout=15)

    print("POSTER URL:", r.url)
    print("POSTER STATUS:", r.status_code)
    print("POSTER RAW (first 300 chars):", r.text[:300])

    try:
        return r.json()
    except Exception:
        return {"error": "poster_non_json", "status": r.status_code}


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "☕ Добро пожаловать в Wave Coffee Rewards!\n\n"
        "Введите номер: Чек Poster № (например 426374), чтобы получить баллы."
    )


import re

import re

async def handle_check(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()

    # Достаём номер чека из любого текста
    m = re.search(r"\d{4,}", text)
    if not m:
        await update.message.reply_text("Введите номер чека Poster, например: 426374")
        return

    receipt_id = m.group(0)

    data = get_transaction(receipt_id)

    # Показать ошибку Poster, если она есть
    if isinstance(data, dict) and "error" in data:
        err = data["error"]
        await update.message.reply_text(
            f"Poster ошибка:\n"
            f"code: {err.get('code')}\n"
            f"message: {err.get('message')}"
        )
        return

    if "response" not in data:
        await update.message.reply_text("Чек не найден в Poster")
        return

    # response может быть list или dict
    resp = data["response"]
    transaction = resp[0] if isinstance(resp, list) and resp else resp

    # Poster часто отдаёт суммы * 100
    raw_total = float(transaction.get("total") or transaction.get("sum") or transaction.get("total_sum") or 0)
    total = raw_total / 100

    cashback = int(total * 0.05)

    async with SessionLocal() as session:
    # Проверяем, был ли уже такой чек
    check = await session.execute(
        text("SELECT id FROM receipts WHERE transaction_id = :tid"),
        {"tid": receipt_id}
    )

    if check.first():
        await update.message.reply_text("⚠️ Этот чек уже активирован.")
        return

    # Создаем пользователя если его нет
    await session.execute(
        text("""
        INSERT INTO users (telegram_id, username)
        VALUES (:tg, :username)
        ON CONFLICT (telegram_id) DO NOTHING
        """),
        {
            "tg": update.effective_user.id,
            "username": update.effective_user.username
        }
    )

    # Сохраняем чек
    await session.execute(
        text("""
        INSERT INTO receipts (transaction_id, telegram_id, amount)
        VALUES (:tid, :tg, :amount)
        """),
        {
            "tid": receipt_id,
            "tg": update.effective_user.id,
            "amount": int(total)
        }
    )

    # Начисляем баланс
    await session.execute(
        text("""
        UPDATE users
        SET balance = balance + :cashback
        WHERE telegram_id = :tg
        """),
        {
            "cashback": cashback,
            "tg": update.effective_user.id
        }
    )

    await session.commit()

    # Получаем новый баланс
    result = await session.execute(
        text("SELECT balance FROM users WHERE telegram_id = :tg"),
        {"tg": update.effective_user.id}
    )

    balance = result.scalar()

await update.message.reply_text(
    f"✅ Чек найден!\n"
    f"Сумма: {int(total)} ₸\n"
    f"Начислено: +{cashback} баллов\n"
    f"Ваш баланс: {balance} баллов"
)
import asyncio
 async def create_tables():
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
def main():
    asyncio.run(create_tables())

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_check))

    print("Bot started...")
    app.run_polling()

if __name__ == "__main__":
   
    main()
