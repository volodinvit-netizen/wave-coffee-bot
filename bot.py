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

REDEEM_TTL_MINUTES = 10

FRIEND_BONUS = 200
LEVEL1_PCT = 0.01
LEVEL2_PCT = 0.02
LEVEL3_PCT = 0.03


# =========================
# ЛОКАЛИЗАЦИЯ
# =========================
TEXTS = {
    "ru": {
        "choose_language": "Выберите язык / Тілді таңдаңыз:",
        "language_saved_ru": "Язык сохранён: Русский 🇷🇺",
        "language_saved_kk": "Тіл сақталды: Қазақша 🇰🇿",
        "main_menu": "☕ Wave Coffee Rewards\n\nНажмите кнопку ниже.",
        "choose_action": "Выберите действие:",
        "reset_done": "Ок. Сбросил. Выберите действие:",
        "balance": "Ваш баланс: {balance} баллов",
        "invite_text": (
            "Ваша ссылка-приглашение:\n"
            "{link}\n\n"
            "Условия:\n"
            "— +{friend_bonus} баллов за друга (когда он впервые активирует чек)\n"
            "— 1 уровень: 1%, 2 уровень: 2%, 3 уровень: 3%"
        ),
        "enter_receipt": "Введите номер чека Poster (только цифры).",
        "receipt_not_visible": "Не вижу номер. Введите номер чека (только цифры).",
        "receipt_not_found": "Чек не найден в Poster. Попробуйте ещё раз или нажмите ❌ Отмена.",
        "receipt_already_used": "⚠️ Этот чек уже активирован.",
        "daily_limit": f"⚠️ Лимит: {DAILY_LIMIT} чека(ов) в день. Попробуйте завтра.",
        "cannot_check_time": "⚠️ Не удалось проверить время чека.\nПожалуйста, активируйте только свежий чек или обратитесь к сотруднику.",
        "receipt_too_old": f"⚠️ Чек уже неактуален (больше {RECEIPT_TTL_MINUTES} минут).\nПожалуйста, активируйте свежий чек.",
        "receipt_found_enter_amount": "✅ Чек найден. Теперь введите сумму чека (числом).",
        "amount_not_understood": "Не понял сумму. Введите просто число, например: 3790",
        "amount_mismatch": (
            "⚠️ Сумма не совпала.\n"
            "Введите сумму с чека ещё раз (числом).\n"
            "Если чек только что пробили — подождите 2–3 секунды и введите снова.\n"
            "Или нажмите ❌ Отмена и начните заново."
        ),
        "state_lost": "Сбилось состояние. Нажмите /start и попробуйте заново.",
        "done": (
            "✅ Готово!\n"
            "Чек: {receipt}\n"
            "Сумма: {amount} ₸\n"
            "Начислено 5%: +{cashback} баллов\n"
            "Ваш баланс: {balance} баллов"
        ),
        "spend_enter_amount": (
            "Введите сумму чека (числом).\n"
            "Оплата баллами возможна только если баллов хватает на 100% суммы."
        ),
        "not_enough_points": (
            "Недостаточно баллов.\n"
            "Нужно: {amount}\n"
            "У вас: {balance}\n\n"
            "Оплата баллами возможна только при 100% покрытии суммы."
        ),
        "code_create_failed": "Не получилось создать код. Попробуйте ещё раз.",
        "redeem_code_created": (
            "✅ Код создан.\n"
            "Сумма: {amount} ₸\n"
            "Код: {code}\n\n"
            "Покажите код кассиру. Код действует {ttl} минут.\n"
            "Баллы спишутся только после подтверждения кассиром."
        ),
        "confirm_admin_only": "Эта кнопка доступна только администратору.",
        "confirm_enter_code": "Введите код (6 цифр), который показал клиент.",
        "confirm_code_format": "Введите код из 6 цифр.",
        "confirm_code_not_found": "Код не найден.",
        "confirm_code_inactive": "Код уже не активен (status={status}).",
        "confirm_code_expired": "Код просрочен.",
        "confirm_not_enough_points": "У клиента недостаточно баллов (баланс изменился).",
        "confirm_done": "✅ Подтверждено. Списано {amount} баллов. Код {code}.",
        "unknown_action": "Не понял действие. Нажмите /start.",
        "unknown_text": "Я не понял. Нажмите кнопку ниже:",
        "menu_balance": "💳 Баланс",
        "menu_earn": "🧾 Начислить по чеку",
        "menu_spend": "💸 Оплатить баллами (100%)",
        "menu_invite": "🤝 Пригласить друга",
        "menu_confirm": "✅ Подтвердить код",
        "menu_cancel": "❌ Отмена",
    },
    "kk": {
        "choose_language": "Тілді таңдаңыз / Выберите язык:",
        "language_saved_ru": "Язык сохранён: Русский 🇷🇺",
        "language_saved_kk": "Тіл сақталды: Қазақша 🇰🇿",
        "main_menu": "☕ Wave Coffee Rewards\n\nТөмендегі батырманы басыңыз.",
        "choose_action": "Әрекетті таңдаңыз:",
        "reset_done": "Жарайды. Тазаланды. Әрекетті таңдаңыз:",
        "balance": "Сіздің балансыңыз: {balance} балл",
        "invite_text": (
            "Сіздің шақыру сілтемеңіз:\n"
            "{link}\n\n"
            "Шарттар:\n"
            "— дос үшін +{friend_bonus} балл (ол чекті алғаш рет белсендіргенде)\n"
            "— 1 деңгей: 1%, 2 деңгей: 2%, 3 деңгей: 3%"
        ),
        "enter_receipt": "Poster чек нөмірін енгізіңіз (тек сандар).",
        "receipt_not_visible": "Чек нөмірі көрінбейді. Тек сандармен енгізіңіз.",
        "receipt_not_found": "Чек Poster жүйесінде табылмады. Қайтадан көріңіз немесе ❌ Болдырмау түймесін басыңыз.",
        "receipt_already_used": "⚠️ Бұл чек бұрын белсендірілген.",
        "daily_limit": f"⚠️ Шектеу: күніне {DAILY_LIMIT} чек. Ертең қайталап көріңіз.",
        "cannot_check_time": "⚠️ Чек уақытын тексеру мүмкін болмады.\nТек жаңа чекті белсендіріңіз немесе қызметкерге хабарласыңыз.",
        "receipt_too_old": f"⚠️ Чек ескірген ({RECEIPT_TTL_MINUTES} минуттан көп).\nТек жаңа чекті белсендіріңіз.",
        "receipt_found_enter_amount": "✅ Чек табылды. Енді чек сомасын енгізіңіз (санмен).",
        "amount_not_understood": "Соманы түсінбедім. Жай санмен енгізіңіз, мысалы: 3790",
        "amount_mismatch": (
            "⚠️ Сома сәйкес келмейді.\n"
            "Чектегі соманы қайта енгізіңіз (санмен).\n"
            "Егер чек жаңа ғана жабылса — 2–3 секунд күтіп, қайта енгізіңіз.\n"
            "Немесе ❌ Болдырмау түймесін басып, қайта бастаңыз."
        ),
        "state_lost": "Күй жоғалды. /start басып, қайтадан көріңіз.",
        "done": (
            "✅ Дайын!\n"
            "Чек: {receipt}\n"
            "Сома: {amount} ₸\n"
            "5% есептелді: +{cashback} балл\n"
            "Сіздің балансыңыз: {balance} балл"
        ),
        "spend_enter_amount": (
            "Чек сомасын енгізіңіз (санмен).\n"
            "Баллмен төлеу тек сома 100% жабылғанда ғана мүмкін."
        ),
        "not_enough_points": (
            "Баллыңыз жеткіліксіз.\n"
            "Қажет: {amount}\n"
            "Сізде: {balance}\n\n"
            "Баллмен төлеу тек соманы 100% жапқанда ғана мүмкін."
        ),
        "code_create_failed": "Код жасау мүмкін болмады. Қайтадан көріңіз.",
        "redeem_code_created": (
            "✅ Код жасалды.\n"
            "Сома: {amount} ₸\n"
            "Код: {code}\n\n"
            "Кодты кассирге көрсетіңіз. Код {ttl} минут жарамды.\n"
            "Балл тек кассир растағаннан кейін ғана шегеріледі."
        ),
        "confirm_admin_only": "Бұл батырма тек әкімшіге қолжетімді.",
        "confirm_enter_code": "Клиент көрсеткен 6 таңбалы кодты енгізіңіз.",
        "confirm_code_format": "6 таңбалы кодты енгізіңіз.",
        "confirm_code_not_found": "Код табылмады.",
        "confirm_code_inactive": "Код енді белсенді емес (status={status}).",
        "confirm_code_expired": "Кодтың уақыты өтіп кеткен.",
        "confirm_not_enough_points": "Клиенттің баллы жеткіліксіз (баланс өзгерген).",
        "confirm_done": "✅ Расталды. {amount} балл шегерілді. Код {code}.",
        "unknown_action": "Әрекет түсініксіз. /start басыңыз.",
        "unknown_text": "Түсінбедім. Төмендегі батырманы басыңыз:",
        "menu_balance": "💳 Баланс",
        "menu_earn": "🧾 Чек бойынша есептеу",
        "menu_spend": "💸 Баллмен төлеу (100%)",
        "menu_invite": "🤝 Дос шақыру",
        "menu_confirm": "✅ Кодты растау",
        "menu_cancel": "❌ Болдырмау",
    },
}


def tr(lang: str, key: str, **kwargs) -> str:
    if lang not in TEXTS:
        lang = "ru"
    text_value = TEXTS[lang][key]
    if kwargs:
        return text_value.format(**kwargs)
    return text_value


# =========================
# БАЗА
# =========================
engine = create_async_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False)


async def get_user_lang(tg_id: int) -> str:
    async with SessionLocal() as session:
        r = await session.execute(
            text("SELECT lang FROM users WHERE telegram_id=:tg"),
            {"tg": tg_id}
        )
        lang = r.scalar()
        return lang or "ru"


async def set_user_lang(tg_id: int, lang: str):
    async with SessionLocal() as session:
        await session.execute(
            text("UPDATE users SET lang=:lang WHERE telegram_id=:tg"),
            {"lang": lang, "tg": tg_id}
        )
        await session.commit()


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
        await conn.execute(text("ALTER TABLE users ADD COLUMN IF NOT EXISTS referrer BIGINT;"))
        await conn.execute(text("ALTER TABLE users ADD COLUMN IF NOT EXISTS friend_bonus_given BOOLEAN DEFAULT FALSE;"))
        await conn.execute(text("ALTER TABLE users ADD COLUMN IF NOT EXISTS lang TEXT DEFAULT 'ru';"))

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

            if s.endswith("Z"):
                try:
                    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)
                except Exception:
                    pass

            if "T" in s and ("+" in s[10:] or "-" in s[10:]):
                try:
                    dt = datetime.fromisoformat(s)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt.astimezone(timezone.utc)
                except Exception:
                    pass

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
        return True
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
def main_menu_keyboard(is_admin: bool, lang: str) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(tr(lang, "menu_balance"), callback_data="menu:balance")],
        [InlineKeyboardButton(tr(lang, "menu_earn"), callback_data="menu:earn")],
        [InlineKeyboardButton(tr(lang, "menu_spend"), callback_data="menu:spend")],
        [InlineKeyboardButton(tr(lang, "menu_invite"), callback_data="menu:invite")],
    ]
    if is_admin:
        rows.append([InlineKeyboardButton(tr(lang, "menu_confirm"), callback_data="menu:confirm")])
    rows.append([InlineKeyboardButton(tr(lang, "menu_cancel"), callback_data="menu:cancel")])
    return InlineKeyboardMarkup(rows)


def language_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Русский 🇷🇺", callback_data="lang:ru"),
            InlineKeyboardButton("Қазақша 🇰🇿", callback_data="lang:kk"),
        ]
    ])


async def show_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, text_msg: str | None = None):
    user = update.effective_user
    lang = await get_user_lang(user.id)
    kb = main_menu_keyboard(user.id == ADMIN_TG_ID, lang)
    if text_msg is None:
        text_msg = tr(lang, "choose_action")

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

    async with SessionLocal() as session:
        r = await session.execute(
            text("SELECT lang FROM users WHERE telegram_id=:tg"),
            {"tg": tg_id}
        )
        lang = r.scalar()

    if not lang:
        await update.message.reply_text(
            tr("ru", "choose_language"),
            reply_markup=language_keyboard()
        )
        return

    await show_menu(update, context, tr(lang, "main_menu"))


async def cancel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    lang = await get_user_lang(update.effective_user.id)
    await show_menu(update, context, tr(lang, "reset_done"))


# =========================
# НАЖАТИЯ КНОПОК
# =========================
async def on_lang_click(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    tg_id = query.from_user.id
    lang = "ru"
    if query.data == "lang:kk":
        lang = "kk"

    await set_user_lang(tg_id, lang)

    saved_msg = "language_saved_ru" if lang == "ru" else "language_saved_kk"
    await query.message.reply_text(tr(lang, saved_msg))
    await query.message.reply_text(tr(lang, "main_menu"), reply_markup=main_menu_keyboard(tg_id == ADMIN_TG_ID, lang))


async def on_menu_click(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    tg_id = query.from_user.id
    username = query.from_user.username
    await ensure_user_exists(tg_id, username)
    lang = await get_user_lang(tg_id)

    action = (query.data or "")

    if action == "menu:cancel":
        context.user_data.clear()
        await query.message.reply_text(tr(lang, "reset_done"), reply_markup=main_menu_keyboard(tg_id == ADMIN_TG_ID, lang))
        return

    if action == "menu:balance":
        async with SessionLocal() as session:
            r = await session.execute(
                text("SELECT balance FROM users WHERE telegram_id=:tg"),
                {"tg": tg_id}
            )
            bal = r.scalar() or 0
        await query.message.reply_text(
            tr(lang, "balance", balance=bal),
            reply_markup=main_menu_keyboard(tg_id == ADMIN_TG_ID, lang)
        )
        return

    if action == "menu:invite":
        bot_username = (await context.bot.get_me()).username
        link = f"https://t.me/{bot_username}?start={tg_id}"
        await query.message.reply_text(
            tr(lang, "invite_text", link=link, friend_bonus=FRIEND_BONUS),
            reply_markup=main_menu_keyboard(tg_id == ADMIN_TG_ID, lang)
        )
        return

    if action == "menu:earn":
        context.user_data.clear()
        context.user_data["mode"] = "earn_wait_receipt"
        await query.message.reply_text(
            tr(lang, "enter_receipt"),
            reply_markup=main_menu_keyboard(tg_id == ADMIN_TG_ID, lang)
        )
        return

    if action == "menu:spend":
        context.user_data.clear()
        context.user_data["mode"] = "spend_wait_amount"
        await query.message.reply_text(
            tr(lang, "spend_enter_amount"),
            reply_markup=main_menu_keyboard(tg_id == ADMIN_TG_ID, lang)
        )
        return

    if action == "menu:confirm":
        if tg_id != ADMIN_TG_ID:
            await query.message.reply_text(tr(lang, "confirm_admin_only"))
            return
        context.user_data.clear()
        context.user_data["mode"] = "confirm_wait_code"
        await query.message.reply_text(
            tr(lang, "confirm_enter_code"),
            reply_markup=main_menu_keyboard(True, lang)
        )
        return

    await query.message.reply_text(tr(lang, "unknown_action"))


# =========================
# ТЕКСТ (по режиму)
# =========================
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tg_id = update.effective_user.id
    username = update.effective_user.username
    await ensure_user_exists(tg_id, username)
    lang = await get_user_lang(tg_id)

    text_msg = (update.message.text or "").strip()
    mode = context.user_data.get("mode")

    if mode == "earn_wait_receipt":
        receipt = parse_receipt(text_msg)
        if not receipt:
            await update.message.reply_text(tr(lang, "receipt_not_visible"))
            return

        data = get_transaction(receipt)
        if not isinstance(data, dict) or "response" not in data:
            await update.message.reply_text(tr(lang, "receipt_not_found"))
            return

        resp = data["response"]
        transaction = resp[0] if isinstance(resp, list) and resp else resp

        total = extract_total_tenge(transaction)
        poster_time = extract_poster_time(transaction)

        async with SessionLocal() as session:
            check = await session.execute(text("SELECT id FROM receipts WHERE transaction_id=:tid"), {"tid": receipt})
            if check.first():
                await update.message.reply_text(tr(lang, "receipt_already_used"))
                return

            if tg_id != ADMIN_TG_ID:
                today = await session.execute(text("""
                    SELECT COUNT(*) FROM receipts
                    WHERE telegram_id=:tg
                      AND created_at >= date_trunc('day', NOW())
                """), {"tg": tg_id})
                if (today.scalar() or 0) >= DAILY_LIMIT:
                    await update.message.reply_text(tr(lang, "daily_limit"))
                    return

                if poster_time is None:
                    await update.message.reply_text(tr(lang, "cannot_check_time"))
                    return

                if is_receipt_too_old(poster_time):
                    await update.message.reply_text(tr(lang, "receipt_too_old"))
                    return

        context.user_data["mode"] = "earn_wait_amount"
        context.user_data["receipt"] = receipt
        context.user_data["poster_sum"] = total
        context.user_data["poster_time"] = poster_time

        await update.message.reply_text(tr(lang, "receipt_found_enter_amount"))
        return

    if mode == "earn_wait_amount":
        amount = parse_amount_tenge(text_msg)
        if amount is None:
            await update.message.reply_text(tr(lang, "amount_not_understood"))
            return

        receipt_id = str(context.user_data.get("receipt", ""))
        if not receipt_id:
            context.user_data.clear()
            await update.message.reply_text(tr(lang, "state_lost"))
            return

        data = get_transaction(receipt_id)
        if not isinstance(data, dict) or "response" not in data:
            await update.message.reply_text(tr(lang, "receipt_not_found"))
            return

        resp = data["response"]
        transaction = resp[0] if isinstance(resp, list) and resp else resp
        expected = extract_total_tenge(transaction)
        poster_time = extract_poster_time(transaction)

        context.user_data["poster_sum"] = expected
        context.user_data["poster_time"] = poster_time

        if abs(amount - expected) > AMOUNT_TOLERANCE_TENGE:
            await update.message.reply_text(tr(lang, "amount_mismatch"))
            return

        if tg_id != ADMIN_TG_ID and poster_time is None:
            context.user_data.clear()
            await update.message.reply_text(tr(lang, "cannot_check_time"))
            return

        if tg_id != ADMIN_TG_ID and is_receipt_too_old(poster_time):
            context.user_data.clear()
            await update.message.reply_text(tr(lang, "receipt_too_old"))
            return

        cashback = int(expected * 0.05)

        async with SessionLocal() as session:
            check = await session.execute(text("SELECT id FROM receipts WHERE transaction_id=:tid"), {"tid": receipt_id})
            if check.first():
                context.user_data.clear()
                await update.message.reply_text(tr(lang, "receipt_already_used"))
                return

            if tg_id != ADMIN_TG_ID:
                today = await session.execute(text("""
                    SELECT COUNT(*) FROM receipts
                    WHERE telegram_id=:tg
                      AND created_at >= date_trunc('day', NOW())
                """), {"tg": tg_id})
                if (today.scalar() or 0) >= DAILY_LIMIT:
                    context.user_data.clear()
                    await update.message.reply_text(tr(lang, "daily_limit"))
                    return

            prev = await session.execute(text("SELECT COUNT(*) FROM receipts WHERE telegram_id=:tg"), {"tg": tg_id})
            first_success = ((prev.scalar() or 0) == 0)

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

            await session.execute(text("""
                INSERT INTO receipts (transaction_id, telegram_id, amount, poster_time)
                VALUES (:tid, :tg, :amount, :poster_time)
            """), {"tid": receipt_id, "tg": tg_id, "amount": expected, "poster_time": poster_time})

            await session.execute(
                text("UPDATE users SET balance = balance + :b WHERE telegram_id=:tg"),
                {"b": cashback, "tg": tg_id}
            )

            if ref1:
                await session.execute(
                    text("UPDATE users SET balance = balance + :b WHERE telegram_id=:tg"),
                    {"b": int(expected * LEVEL1_PCT), "tg": ref1}
                )
            if ref2:
                await session.execute(
                    text("UPDATE users SET balance = balance + :b WHERE telegram_id=:tg"),
                    {"b": int(expected * LEVEL2_PCT), "tg": ref2}
                )
            if ref3:
                await session.execute(
                    text("UPDATE users SET balance = balance + :b WHERE telegram_id=:tg"),
                    {"b": int(expected * LEVEL3_PCT), "tg": ref3}
                )

            if first_success and ref1 and (not friend_bonus_given):
                await session.execute(
                    text("UPDATE users SET balance = balance + :b WHERE telegram_id=:tg"),
                    {"b": FRIEND_BONUS, "tg": ref1}
                )
                await session.execute(
                    text("UPDATE users SET friend_bonus_given = TRUE WHERE telegram_id=:tg"),
                    {"tg": tg_id}
                )

            await session.commit()

            rbal = await session.execute(text("SELECT balance FROM users WHERE telegram_id=:tg"), {"tg": tg_id})
            new_balance = rbal.scalar() or 0

        context.user_data.clear()
        await update.message.reply_text(
            tr(lang, "done", receipt=receipt_id, amount=expected, cashback=cashback, balance=new_balance)
        )
        await show_menu(update, context, tr(lang, "choose_action"))
        return

    if mode == "spend_wait_amount":
        amount = parse_amount_tenge(text_msg)
        if amount is None or amount <= 0:
            await update.message.reply_text(tr(lang, "amount_not_understood"))
            return

        async with SessionLocal() as session:
            r = await session.execute(text("SELECT balance FROM users WHERE telegram_id=:tg"), {"tg": tg_id})
            bal = r.scalar() or 0

            if bal < amount:
                context.user_data.clear()
                await update.message.reply_text(tr(lang, "not_enough_points", amount=amount, balance=bal))
                await show_menu(update, context, tr(lang, "choose_action"))
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
                await update.message.reply_text(tr(lang, "code_create_failed"))
                return

        context.user_data.clear()
        await update.message.reply_text(
            tr(lang, "redeem_code_created", amount=amount, code=code, ttl=REDEEM_TTL_MINUTES)
        )
        await show_menu(update, context, tr(lang, "choose_action"))
        return

    if mode == "confirm_wait_code":
        if tg_id != ADMIN_TG_ID:
            context.user_data.clear()
            await update.message.reply_text(tr(lang, "confirm_admin_only"))
            return

        m = re.search(r"\d{6}", text_msg)
        if not m:
            await update.message.reply_text(tr(lang, "confirm_code_format"))
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
                await update.message.reply_text(tr(lang, "confirm_code_not_found"))
                return

            rid, user_id, amount, status, created_at = row

            if status != "pending":
                await update.message.reply_text(tr(lang, "confirm_code_inactive", status=status))
                return

            now = datetime.now(timezone.utc)
            if created_at is not None and (now - created_at) > timedelta(minutes=REDEEM_TTL_MINUTES):
                await session.execute(text("UPDATE redemptions SET status='expired' WHERE id=:id"), {"id": rid})
                await session.commit()
                await update.message.reply_text(tr(lang, "confirm_code_expired"))
                return

            r2 = await session.execute(text("SELECT balance FROM users WHERE telegram_id=:tg"), {"tg": user_id})
            bal = r2.scalar() or 0

            if bal < amount:
                await update.message.reply_text(tr(lang, "confirm_not_enough_points"))
                return

            await session.execute(
                text("UPDATE users SET balance = balance - :amt WHERE telegram_id = :tg"),
                {"amt": amount, "tg": user_id}
            )
            await session.execute(
                text("UPDATE redemptions SET status='used', used_at=NOW() WHERE id=:id"),
                {"id": rid}
            )
            await session.commit()

        context.user_data.clear()
        await update.message.reply_text(tr(lang, "confirm_done", amount=amount, code=code))
        await show_menu(update, context, tr(lang, "choose_action"))
        return

    await show_menu(update, context, tr(lang, "unknown_text"))


async def on_startup(app):
    await create_or_update_tables()


def main():
    app = Application.builder().token(BOT_TOKEN).post_init(on_startup).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("cancel", cancel_cmd))

    app.add_handler(CallbackQueryHandler(on_lang_click, pattern=r"^lang:"))
    app.add_handler(CallbackQueryHandler(on_menu_click, pattern=r"^menu:"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    print("Bot started...")
    app.run_polling()


if __name__ == "__main__":
    main()
