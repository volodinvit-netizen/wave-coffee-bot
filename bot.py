import os
import requests
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

BOT_TOKEN = os.getenv("BOT_TOKEN")
POSTER_TOKEN = os.getenv("POSTER_TOKEN")
POSTER_DOMAIN = os.getenv("POSTER_DOMAIN")

BASE_URL = f"https://{POSTER_DOMAIN}/api/v2"


def get_transaction(transaction_id: str):
    url = f"{BASE_URL}/dash.getTransaction"
    r = requests.get(url, params={
        "token": POSTER_TOKEN,
        "transaction_id": transaction_id
    })
    return r.json()


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "☕ Добро пожаловать в Wave Coffee Rewards!\n\n"
        "Введите номер: Чек Poster № (например 426374), чтобы получить баллы."
    )


async def handle_check(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()

    if not text.isdigit():
        await update.message.reply_text("Введите только номер чека, например: 426374")
        return

    receipt_id = text

    data = get_transaction(receipt_id)

    if "response" not in data:
        await update.message.reply_text("Чек не найден. Проверь номер.")
        return

    transaction = data["response"]

    total = float(transaction.get("total", 0))

    cashback = int(total * 0.05)

    await update.message.reply_text(
        f"✅ Чек найден!\n"
        f"Сумма: {int(total)} ₸\n"
        f"Начислено кешбэком 5%: +{cashback} баллов\n\n"
        f"(Скоро добавим баланс, рефералов и защиту от повторов)"
    )


import asyncio

def main():
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_check))

    print("Bot started...")
    app.run_polling(close_loop=False)


if __name__ == "__main__":
    main()
