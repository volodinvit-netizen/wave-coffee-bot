import os
import re
import requests
import secrets
from datetime import datetime, timedelta, timezone

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes,
)

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
AMOUNT_TOLERANCE_TENGE = 2

# списание
REDEEM_TTL_MINUTES = 10  # код действует 10 минут

# рефералка
FRIEND_BONUS = 200
LEVEL1_PCT = 0.03
LEVEL2_PCT = 0.02
LEVEL3_PCT = 0.01


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

        await conn.execute(text("ALTER TABLE receipts ADD COLUMN IF NOT EXISTS poster_time TIMESTAMPTZ;"))

        # Рефералка
        await conn.execute(text("ALTER TABLE users ADD COLUMN IF NOT EXISTS referrer BIGINT;"))
        await conn.execute(text("ALTER TABLE users ADD COLUMN IF NOT EXISTS friend_bonus_given BOOLEAN DEFAULT FALSE;"))

        # Списание
        await conn.execute(text("""
        CREATE TABLE IF NOT EXISTS redemptions (
            id BIGSERIAL PRIMARY KEY,
            code TEXT UNIQUE,
            telegram_id BIGINT,
            amount BIGINT,
            status TEXT DEFAULT 'pending',
            created_at TIMESTAMPTZ DEFAULT NOW(),
            used_at TIMESTAMPTZ
        );
        """))


async def ensure_user_exists(tg_id: int, username: str | None):
    async with SessionLocal() as session:
        await session.execute(text("""
        INSERT INTO users (telegram_id, username)
        VALUES (:tg, :username)
        ON CONFLICT (telegram_id) DO NOTHING
        """), {"tg": tg_id, "username": username})
        await session.commit()


async def set_referrer_if_empty(tg_id: int, referrer_id: int | None):
    if not referrer_id or referrer_id == tg_id:
        return

    async with SessionLocal() as session:
        await session.execute(text("""
            UPDATE users
            SET referrer = COALESCE(referrer, :ref)
            WHERE telegram_id = :tg
        """), {"ref": referrer_id, "tg": tg_id})
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
# ПАРСИНГ
# =========================
def parse_receipt(text_msg: str) -> str | None:
    m = re.search(r"\d{4,}", text_msg or "")
    return m.group(0) if m else None


def parse_amount_tenge(text_msg: str) -> int | None:
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


def generate_code() -> str:
    return str(secrets.randbelow(900000) + 100000)


# =========================
# КНОПКИ / МЕНЮ
# =========================
def main_menu_keyboard(is_admin: bool) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("💳 Баланс", callback_data="menu:balance")],
        [InlineKeyboardButton("🧾 Начислить по чеку", callback_data="menu:earn")],
        [InlineKeyboardButton("💸 Оплатить баллами (100%)", callback_data="menu:spend")],
        [InlineKeyboardButton("🤝 Пригласить друга", callback_data="menu:invite")],
    ]
    if is_admin:
        rows.append([InlineKeyboardButton("✅ Подтвердить код", callback_data="menu:confirm")])
    rows.append([InlineKeyboardButton("❌ Отмена", callback_data="menu:cancel")])
    return InlineKeyboardMarkup(rows)


async def show_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, text_msg: str = "Выберите действие:"):
    user = update.effective_user
    kb = main_menu_keyboard(user.id == ADMIN_TG_ID)

    if update.message:
        await update.message.reply_text(text_msg, reply_markup=kb)
    else:
        await update.callback_query.message.reply_text(text_msg, reply_markup=kb)


# =========================
# /start
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tg_id = update.effective_user.id
    username = update.effective_user.username
    await ensure_user_exists(tg_id, username)

    ref = None
    if context.args:
        try:
            ref = int(context.args[0])
        except Exception:
            ref = None

    await set_referrer_if_empty(tg_id, ref)

    context.user_data.clear()
    await show_menu(update, context, "☕ Wave Coffee Rewards\n\nНажмите кнопку ниже.")


async def cancel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await show_menu(update, context, "Ок. Сбросил. Выберите действие:")


# =========================
# НАЖАТИЯ КНОПОК
# =========================
async def on_menu_click(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    tg_id = query.from_user.id
    username = query.from_user.username
    await ensure_user_exists(tg_id, username)

    action = (query.data or "")

    if action == "menu:cancel":
        context.user_data.clear()
        await query.message.reply_text("Ок. Сбросил.", reply_markup=main_menu_keyboard(tg_id == ADMIN_TG_ID))
        return

    if action == "menu:balance":
        async with SessionLocal() as session:
            r = await session.execute(
                text("SELECT balance FROM users WHERE telegram_id=:tg"),
                {"tg": tg_id}
            )
            bal = r.scalar() or 0
        await query.message.reply_text(f"Ваш баланс: {bal} баллов", reply_markup=main_menu_keyboard(tg_id == ADMIN_TG_ID))
        return

    if action == "menu:invite":
        bot_username = (await context.bot.get_me()).username
        link = f"https://t.me/{bot_username}?start={tg_id}"
        await query.message.reply_text(
            "Ваша ссылка-приглашение:\n"
            f"{link}\n\n"
            f"Условия:\n"
            f"— +{FRIEND_BONUS} баллов за друга (когда он впервые активирует чек)\n"
            f"— 1 уровень: 3%, 2 уровень: 2%, 3 уровень: 1%",
            reply_markup=main_menu_keyboard(tg_id == ADMIN_TG_ID)
        )
        return

    if action == "menu:earn":
        context.user_data.clear()
        context.user_data["mode"] = "earn_wait_receipt"
        await query.message.reply_text("Введите номер чека Poster (только цифры).", reply_markup=main_menu_keyboard(tg_id == ADMIN_TG_ID))
        return

    if action == "menu:spend":
        context.user_data.clear()
        context.user_data["mode"] = "spend_wait_amount"
        await query.message.reply_text(
            "Введите сумму чека (числом).\n"
            "Оплата баллами возможна только если баллов хватает на 100% суммы.",
            reply_markup=main_menu_keyboard(tg_id == ADMIN_TG_ID)
        )
        return

    if action == "menu:confirm":
        if tg_id != ADMIN_TG_ID:
            await query.message.reply_text("Эта кнопка доступна только администратору.")
            return
        context.user_data.clear()
        context.user_data["mode"] = "confirm_wait_code"
        await query.message.reply_text("Введите код (6 цифр), который показал клиент.", reply_markup=main_menu_keyboard(True))
        return

    await query.message.reply_text("Не понял действие. Нажмите /start.")


# =========================
# ТЕКСТ (по режиму)
# =========================
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tg_id = update.effective_user.id
    username = update.effective_user.username
    await ensure_user_exists(tg_id, username)

    text_msg = (update.message.text or "").strip()
    mode = context.user_data.get("mode")

    # --- 1) Начисление: ждём номер чека
    if mode == "earn_wait_receipt":
        receipt = parse_receipt(text_msg)
        if not receipt:
            await update.message.reply_text("Не вижу номер. Введите номер чека (только цифры).")
            return

        data = get_transaction(receipt)
        if not isinstance(data, dict) or "response" not in data:
            await update.message.reply_text("Чек не найден в Poster. Попробуйте ещё раз или нажмите ❌ Отмена.")
            return

        resp = data["response"]
        transaction = resp[0] if isinstance(resp, list) and resp else resp

        total = extract_total_tenge(transaction)
        poster_time = extract_poster_time(transaction)

        async with SessionLocal() as session:
            check = await session.execute(text("SELECT id FROM receipts WHERE transaction_id=:tid"), {"tid": receipt})
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

        context.user_data["mode"] = "earn_wait_amount"
        context.user_data["receipt"] = receipt
        context.user_data["poster_sum"] = total
        context.user_data["poster_time"] = poster_time

        await update.message.reply_text("✅ Чек найден. Теперь введите сумму чека (числом).")
        return

    # --- 2) Начисление: ждём сумму
    if mode == "earn_wait_amount":
        amount = parse_amount_tenge(text_msg)
        if amount is None:
            await update.message.reply_text("Не понял сумму. Введите просто число, например: 3790")
            return

        receipt_id = str(context.user_data.get("receipt", ""))
        if not receipt_id:
            context.user_data.clear()
            await update.message.reply_text("Сбилось состояние. Нажмите /start и попробуйте заново.")
            return

        # ✅ ВОТ ГЛАВНАЯ ПРАВКА:
        # каждый раз, когда человек вводит сумму — заново тянем чек из Poster,
        # чтобы expected был актуальный
        data = get_transaction(receipt_id)
        if not isinstance(data, dict) or "response" not in data:
            await update.message.reply_text("Не смог обновить чек в Poster. Попробуйте ещё раз.")
            return

        resp = data["response"]
        transaction = resp[0] if isinstance(resp, list) and resp else resp
        expected = extract_total_tenge(transaction)
        poster_time = extract_poster_time(transaction)

        # обновим в памяти (на всякий)
        context.user_data["poster_sum"] = expected
        context.user_data["poster_time"] = poster_time

        if abs(amount - expected) > AMOUNT_TOLERANCE_TENGE:
            await update.message.reply_text(
                "⚠️ Сумма не совпала.\n"
                "Введите сумму с чека ещё раз (числом).\n"
                "Если чек только что пробили — подождите 2–3 секунды и введите снова.\n"
                "Или нажмите ❌ Отмена и начните заново."
            )
            return

        # 10 минут — админ обходит
        if tg_id != ADMIN_TG_ID and is_receipt_too_old(poster_time):
            context.user_data.clear()
            await update.message.reply_text(
                f"⚠️ Чек уже неактуален (больше {RECEIPT_TTL_MINUTES} минут).\n"
                "Нажмите /start и попробуйте заново."
            )
            return

        cashback = int(expected * 0.05)

        async with SessionLocal() as session:
            # дубль
            check = await session.execute(text("SELECT id FROM receipts WHERE transaction_id=:tid"), {"tid": receipt_id})
            if check.first():
                context.user_data.clear()
                await update.message.reply_text("⚠️ Этот чек уже активирован.")
                return

            # лимит — админ обходит
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

            # первая успешная активация?
            prev = await session.execute(text("SELECT COUNT(*) FROM receipts WHERE telegram_id=:tg"), {"tg": tg_id})
            first_success = ((prev.scalar() or 0) == 0)

            # цепочка рефереров
            r = await session.execute(
                text("SELECT referrer, friend_bonus_given FROM users WHERE telegram_id=:tg"),
                {"tg": tg_id}
            )
            row = r.first()
            ref1 = row[0] if row else None
            friend_bonus_given = bool(row[1]) if row else False

            ref2 = None
            ref3 = None

            if ref1:
                r2 = await session.execute(text("SELECT referrer FROM users WHERE telegram_id=:tg"), {"tg": ref1})
                ref2 = r2.scalar()

            if ref2:
                r3 = await session.execute(text("SELECT referrer FROM users WHERE telegram_id=:tg"), {"tg": ref2})
                ref3 = r3.scalar()

            # сохраняем чек
            await session.execute(text("""
                INSERT INTO receipts (transaction_id, telegram_id, amount, poster_time)
                VALUES (:tid, :tg, :amount, :poster_time)
            """), {"tid": receipt_id, "tg": tg_id, "amount": expected, "poster_time": poster_time})

            # 5% пользователю
            await session.execute(text("UPDATE users SET balance = balance + :b WHERE telegram_id=:tg"),
                                  {"b": cashback, "tg": tg_id})

            # проценты по уровням
            if ref1:
                await session.execute(text("UPDATE users SET balance = balance + :b WHERE telegram_id=:tg"),
                                      {"b": int(expected * LEVEL1_PCT), "tg": ref1})
            if ref2:
                await session.execute(text("UPDATE users SET balance = balance + :b WHERE telegram_id=:tg"),
                                      {"b": int(expected * LEVEL2_PCT), "tg": ref2})
            if ref3:
                await session.execute(text("UPDATE users SET balance = balance + :b WHERE telegram_id=:tg"),
                                      {"b": int(expected * LEVEL3_PCT), "tg": ref3})

            # +200 за друга 1 раз
            if first_success and ref1 and (not friend_bonus_given):
                await session.execute(text("UPDATE users SET balance = balance + :b WHERE telegram_id=:tg"),
                                      {"b": FRIEND_BONUS, "tg": ref1})
                await session.execute(text("UPDATE users SET friend_bonus_given = TRUE WHERE telegram_id=:tg"),
                                      {"tg": tg_id})

            await session.commit()

            rbal = await session.execute(text("SELECT balance FROM users WHERE telegram_id=:tg"), {"tg": tg_id})
            new_balance = rbal.scalar() or 0

        context.user_data.clear()
        await update.message.reply_text(
            "✅ Готово!\n"
            f"Чек: {receipt_id}\n"
            f"Сумма: {expected} ₸\n"
            f"Начислено 5%: +{cashback} баллов\n"
            f"Ваш баланс: {new_balance} баллов"
        )
        await show_menu(update, context, "Что дальше?")
        return

    # --- 3) Списание: ждём сумму
    if mode == "spend_wait_amount":
        amount = parse_amount_tenge(text_msg)
        if amount is None or amount <= 0:
            await update.message.reply_text("Не понял сумму. Введите просто число, например: 3790")
            return

        async with SessionLocal() as session:
            r = await session.execute(text("SELECT balance FROM users WHERE telegram_id=:tg"), {"tg": tg_id})
            bal = r.scalar() or 0

            if bal < amount:
                context.user_data.clear()
                await update.message.reply_text(
                    f"Недостаточно баллов.\nНужно: {amount}\nУ вас: {bal}\n\n"
                    f"Оплата баллами возможна только при 100% покрытии суммы."
                )
                await show_menu(update, context, "Выберите действие:")
                return

            code = generate_code()
            for _ in range(2):
                try:
                    await session.execute(text("""
                        INSERT INTO redemptions (code, telegram_id, amount, status)
                        VALUES (:code, :tg, :amt, 'pending')
                    """), {"code": code, "tg": tg_id, "amt": amount})
                    await session.commit()
                    break
                except Exception:
                    code = generate_code()
            else:
                context.user_data.clear()
                await update.message.reply_text("Не получилось создать код. Попробуйте ещё раз.")
                return

        context.user_data.clear()
        await update.message.reply_text(
            "✅ Код создан.\n"
            f"Сумма: {amount} ₸\n"
            f"Код: {code}\n\n"
            f"Покажите код кассиру. Код действует {REDEEM_TTL_MINUTES} минут.\n"
            f"Баллы спишутся только после подтверждения кассиром."
        )
        await show_menu(update, context, "Выберите действие:")
        return

    # --- 4) Подтверждение: ждём код (админ)
    if mode == "confirm_wait_code":
        if tg_id != ADMIN_TG_ID:
            context.user_data.clear()
            await update.message.reply_text("Эта функция доступна только администратору.")
            return

        m = re.search(r"\d{6}", text_msg)
        if not m:
            await update.message.reply_text("Введите код из 6 цифр.")
            return

        code = m.group(0)

        async with SessionLocal() as session:
            r = await session.execute(text("""
                SELECT id, telegram_id, amount, status, created_at
                FROM redemptions
                WHERE code = :code
            """), {"code": code})
            row = r.first()

            if not row:
                await update.message.reply_text("Код не найден.")
                return

            rid, user_id, amount, status, created_at = row

            if status != "pending":
                await update.message.reply_text(f"Код уже не активен (status={status}).")
                return

            now = datetime.now(timezone.utc)
            if created_at is not None and (now - created_at) > timedelta(minutes=REDEEM_TTL_MINUTES):
                await session.execute(text("UPDATE redemptions SET status='expired' WHERE id=:id"), {"id": rid})
                await session.commit()
                await update.message.reply_text("Код просрочен.")
                return

            r2 = await session.execute(text("SELECT balance FROM users WHERE telegram_id=:tg"), {"tg": user_id})
            bal = r2.scalar() or 0

            if bal < amount:
                await update.message.reply_text("У клиента недостаточно баллов (баланс изменился).")
                return

            await session.execute(text("UPDATE users SET balance = balance - :amt WHERE telegram_id = :tg"),
                                  {"amt": amount, "tg": user_id})
            await session.execute(text("UPDATE redemptions SET status='used', used_at=NOW() WHERE id=:id"),
                                  {"id": rid})
            await session.commit()

        context.user_data.clear()
        await update.message.reply_text(f"✅ Подтверждено. Списано {amount} баллов. Код {code}.")
        await show_menu(update, context, "Выберите действие:")
        return

    await show_menu(update, context, "Я не понял. Нажмите кнопку ниже:")


async def on_startup(app):
    await create_or_update_tables()


def main():
    app = Application.builder().token(BOT_TOKEN).post_init(on_startup).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("cancel", cancel_cmd))

    app.add_handler(CallbackQueryHandler(on_menu_click, pattern=r"^menu:"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    print("Bot started...")
    app.run_polling()


if __name__ == "__main__":
    main()
