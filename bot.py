import os
import re
import requests
import secrets
from datetime import datetime, timedelta, timezone, time

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
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

CASHIER_TG_IDS_RAW = os.getenv("CASHIER_TG_IDS", "")
CASHIER_TG_IDS = set()
for part in CASHIER_TG_IDS_RAW.split(","):
    part = part.strip()
    if part.isdigit():
        CASHIER_TG_IDS.add(int(part))

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
AMOUNT_TOLERANCE_TENGE = 5
REDEEM_TTL_MINUTES = 10

# Новые retention-настройки
SLEEP_DAYS = 7
FIRST_RETURN_AFTER_DAYS = 2
TEMP_BONUS_AMOUNT = 100
TEMP_BONUS_HOURS = 48
STATUS_REMINDER_THRESHOLD = 3
STATUS_REMINDER_COOLDOWN_DAYS = 7
SLEEP_BONUS_COOLDOWN_DAYS = 7

FRIEND_BONUS = 200
LEVEL1_PCT = 0.01
LEVEL2_PCT = 0.02
LEVEL3_PCT = 0.03

LOCAL_TZ = timezone(timedelta(hours=5))  # Актау / Казахстан

STATUS_RULES = [
    ("Explorer", 0),
    ("Regular", 5),
    ("Legend", 20),
    ("Icon", 100),
]


# =========================
# ПОДКЛЮЧЕНИЕ К БАЗЕ
# =========================
engine = create_async_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False)


# =========================
# ПРАВА ДОСТУПА
# =========================
def is_admin(tg_id: int) -> bool:
    return tg_id == ADMIN_TG_ID


def is_cashier(tg_id: int) -> bool:
    return tg_id in CASHIER_TG_IDS


def is_staff(tg_id: int) -> bool:
    return is_admin(tg_id) or is_cashier(tg_id)


# =========================
# ЛОКАЛИЗАЦИЯ
# =========================
TEXTS = {
    "ru": {
        "choose_language": "Выберите язык:",
        "language_saved_ru": "Язык сохранён: Русский",
        "language_saved_kk": "Тіл сақталды: Қазақша",
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
        "confirm_staff_only": "Эта кнопка доступна только сотруднику.",
        "confirm_enter_code": "Введите код (6 цифр), который показал клиент.",
        "confirm_code_format": "Введите код из 6 цифр.",
        "confirm_code_not_found": "Код не найден.",
        "confirm_code_inactive": "Код уже не активен (status={status}).",
        "confirm_code_expired": "Код просрочен.",
        "confirm_not_enough_points": "У клиента недостаточно баллов (баланс изменился).",
        "confirm_done": "✅ Подтверждено. Списано {amount} баллов. Код {code}.",
        "stats_staff_only": "Статистика доступна только сотруднику.",
        "stats_text": (
            "📊 Статистика\n\n"
            "Сегодня:\n"
            "— Чеков: {today_receipts}\n"
            "— Новых клиентов: {today_users}\n"
            "— Сумма чеков: {today_amount} ₸\n"
            "— Начислено баллов: {today_cashback}\n"
            "— Списано баллов: {today_spent}\n"
            "— Вернувшихся клиентов: {returning_users}\n\n"
            "Всего:\n"
            "— Клиентов: {all_users}\n"
            "— Чеков: {all_receipts}\n"
            "— Сумма чеков: {all_amount} ₸\n"
            "— Начислено баллов: {all_cashback}\n"
            "— Списано баллов: {all_spent}"
        ),
        "client_staff_only": "Карточка клиента доступна только сотруднику.",
        "client_enter_query": "Введите @username или telegram_id клиента.",
        "client_not_found": "Клиент не найден.",
        "client_card": (
            "👤 Клиент\n\n"
            "ID: {telegram_id}\n"
            "Username: {username}\n"
            "Баланс: {balance} баллов\n"
            "Статус: {status}\n"
            "Чеков: {receipts_count}\n"
            "Сумма чеков: {receipts_amount} ₸\n"
            "Списано: {spent_amount} баллов\n"
            "Последний визит: {last_visit}\n"
            "Пригласил: {referrer}"
        ),
        "recent_receipts_staff_only": "Список чеков доступен только сотруднику.",
        "recent_receipts_empty": "Пока нет активированных чеков.",
        "recent_receipts_title": "🕘 Последние чеки\n\n{items}",
        "recent_redemptions_staff_only": "Список списаний доступен только сотруднику.",
        "recent_redemptions_empty": "Пока нет списаний.",
        "recent_redemptions_title": "💸 Последние списания\n\n{items}",
        "unknown_action": "Не понял действие. Нажмите /start.",
        "unknown_text": "Я не понял. Нажмите кнопку ниже:",
        "status_text": "🏆 Ваш статус: {status}\nЧеков: {count}\nДо следующего уровня: {remaining}",
        "status_upgraded": "🏆 Новый статус: {status}",
        "sleep_bonus_message": "☕ Мы давно вас не видели. Дарим +100 бонусов. Они действуют 48 часов.",
        "first_return_bonus_message": "☕ Спасибо за первый визит. Возвращайтесь — держите +100 бонусов на 48 часов.",
        "status_reminder_message": "🏆 До статуса {status} осталось всего {remaining} чек(а/ов).",
        "adjust_admin_only": "Эта функция доступна только администратору.",
        "adjust_enter_client": "Введите @username или telegram_id клиента для корректировки баланса.",
        "adjust_enter_delta": "Введите изменение баланса. Например: 200 или -150",
        "adjust_enter_reason": "Введите причину корректировки.",
        "adjust_done": "✅ Готово. Клиент: {user}\nИзменение: {delta}\nНовый баланс: {balance}\nПричина: {reason}",
        "adjust_notice": "ℹ️ Баланс изменён администратором.\nИзменение: {delta}\nНовый баланс: {balance}\nПричина: {reason}",
        "menu_balance": "💳 Баланс",
        "menu_earn": "🧾 Начислить по чеку",
        "menu_spend": "💸 Оплатить баллами (100%)",
        "menu_invite": "🤝 Пригласить друга",
        "menu_language": "🌐 Сменить язык",
        "menu_stats": "📊 Статистика",
        "menu_confirm": "✅ Подтвердить код",
        "menu_client": "👤 Клиент",
        "menu_recent_receipts": "🕘 Последние чеки",
        "menu_recent_redemptions": "💸 Последние списания",
        "menu_status": "🏆 Мой статус",
        "menu_adjust": "➕➖ Корректировка баланса",
        "menu_cancel": "❌ Отмена",
    },
    "kk": {
        "choose_language": "Тілді таңдаңыз:",
        "language_saved_ru": "Язык сохранён: Русский",
        "language_saved_kk": "Тіл сақталды: Қазақша",
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
        "confirm_staff_only": "Бұл батырма тек қызметкерге қолжетімді.",
        "confirm_enter_code": "Клиент көрсеткен 6 таңбалы кодты енгізіңіз.",
        "confirm_code_format": "6 таңбалы кодты енгізіңіз.",
        "confirm_code_not_found": "Код табылмады.",
        "confirm_code_inactive": "Код енді белсенді емес (status={status}).",
        "confirm_code_expired": "Кодтың уақыты өтіп кеткен.",
        "confirm_not_enough_points": "Клиенттің баллы жеткіліксіз (баланс өзгерген).",
        "confirm_done": "✅ Расталды. {amount} балл шегерілді. Код {code}.",
        "stats_staff_only": "Статистика тек қызметкерге қолжетімді.",
        "stats_text": (
            "📊 Статистика\n\n"
            "Бүгін:\n"
            "— Чектер: {today_receipts}\n"
            "— Жаңа клиенттер: {today_users}\n"
            "— Чек сомасы: {today_amount} ₸\n"
            "— Есептелген балл: {today_cashback}\n"
            "— Шегерілген балл: {today_spent}\n"
            "— Қайта келген клиенттер: {returning_users}\n\n"
            "Барлығы:\n"
            "— Клиенттер: {all_users}\n"
            "— Чектер: {all_receipts}\n"
            "— Чек сомасы: {all_amount} ₸\n"
            "— Есептелген балл: {all_cashback}\n"
            "— Шегерілген балл: {all_spent}"
        ),
        "client_staff_only": "Клиент картасы тек қызметкерге қолжетімді.",
        "client_enter_query": "@username немесе telegram_id енгізіңіз.",
        "client_not_found": "Клиент табылмады.",
        "client_card": (
            "👤 Клиент\n\n"
            "ID: {telegram_id}\n"
            "Username: {username}\n"
            "Баланс: {balance} балл\n"
            "Мәртебе: {status}\n"
            "Чектер саны: {receipts_count}\n"
            "Чек сомасы: {receipts_amount} ₸\n"
            "Шегерілгені: {spent_amount} балл\n"
            "Соңғы келуі: {last_visit}\n"
            "Шақырған адам: {referrer}"
        ),
        "recent_receipts_staff_only": "Чектер тізімі тек қызметкерге қолжетімді.",
        "recent_receipts_empty": "Әзірге белсендірілген чек жоқ.",
        "recent_receipts_title": "🕘 Соңғы чектер\n\n{items}",
        "recent_redemptions_staff_only": "Списания тізімі тек қызметкерге қолжетімді.",
        "recent_redemptions_empty": "Әзірге списания жоқ.",
        "recent_redemptions_title": "💸 Соңғы списаниялар\n\n{items}",
        "unknown_action": "Әрекет түсініксіз. /start басыңыз.",
        "unknown_text": "Түсінбедім. Төмендегі батырманы басыңыз:",
        "status_text": "🏆 Сіздің мәртебеңіз: {status}\nЧектер: {count}\nКелесі деңгейге дейін: {remaining}",
        "status_upgraded": "🏆 Жаңа мәртебе: {status}",
        "sleep_bonus_message": "☕ Сізді көптен бері көрмедік. +100 бонус береміз. Олар 48 сағат жарамды.",
        "first_return_bonus_message": "☕ Алғашқы келуіңізге рақмет. Қайта келіңіз — 48 сағатқа +100 бонус.",
        "status_reminder_message": "🏆 {status} мәртебесіне дейін тек {remaining} чек қалды.",
        "adjust_admin_only": "Бұл функция тек әкімшіге қолжетімді.",
        "adjust_enter_client": "Балансты түзету үшін @username немесе telegram_id енгізіңіз.",
        "adjust_enter_delta": "Баланс өзгерісін енгізіңіз. Мысалы: 200 немесе -150",
        "adjust_enter_reason": "Түзету себебін енгізіңіз.",
        "adjust_done": "✅ Дайын. Клиент: {user}\nӨзгеріс: {delta}\nЖаңа баланс: {balance}\nСебеп: {reason}",
        "adjust_notice": "ℹ️ Балансты әкімші өзгертті.\nӨзгеріс: {delta}\nЖаңа баланс: {balance}\nСебеп: {reason}",
        "menu_balance": "💳 Баланс",
        "menu_earn": "🧾 Чек бойынша есептеу",
        "menu_spend": "💸 Баллмен төлеу (100%)",
        "menu_invite": "🤝 Дос шақыру",
        "menu_language": "🌐 Тілді өзгерту",
        "menu_stats": "📊 Статистика",
        "menu_confirm": "✅ Кодты растау",
        "menu_client": "👤 Клиент",
        "menu_recent_receipts": "🕘 Соңғы чектер",
        "menu_recent_redemptions": "💸 Соңғы списания",
        "menu_status": "🏆 Менің мәртебем",
        "menu_adjust": "➕➖ Балансты түзету",
        "menu_cancel": "❌ Болдырмау",
    },
}


def tr(lang: str, key: str, **kwargs):
    if lang not in TEXTS:
        lang = "ru"
    value = TEXTS[lang][key]
    return value.format(**kwargs) if kwargs else value


# =========================
# ВСПОМОГАТЕЛЬНОЕ
# =========================
def format_dt_local(value: datetime | None) -> str:
    if not value:
        return "—"
    return value.astimezone(LOCAL_TZ).strftime("%d.%m.%Y %H:%M")


def get_status_by_receipts(count: int) -> tuple[str, int | None]:
    current = STATUS_RULES[0][0]
    next_threshold = None
    for idx, (name, threshold) in enumerate(STATUS_RULES):
        if count >= threshold:
            current = name
            if idx + 1 < len(STATUS_RULES):
                next_threshold = STATUS_RULES[idx + 1][1]
        else:
            next_threshold = threshold
            break
    remaining = None if next_threshold is None else max(0, next_threshold - count)
    return current, remaining


def get_next_status_info(count: int) -> tuple[str | None, int | None]:
    for name, threshold in STATUS_RULES:
        if count < threshold:
            return name, threshold - count
    return None, None


def generate_code() -> str:
    return str(secrets.randbelow(900000) + 100000)


# =========================
# БАЗА
# =========================
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
        await conn.execute(text("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_visit TIMESTAMPTZ;"))
        await conn.execute(text("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_retention_sent_at TIMESTAMPTZ;"))
        await conn.execute(text("ALTER TABLE users ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'Explorer';"))

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

        await conn.execute(text("""
        CREATE TABLE IF NOT EXISTS balance_adjustments (
            id BIGSERIAL PRIMARY KEY,
            target_telegram_id BIGINT NOT NULL,
            admin_telegram_id BIGINT NOT NULL,
            delta BIGINT NOT NULL,
            reason TEXT NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        """))

        await conn.execute(text("""
        CREATE TABLE IF NOT EXISTS bonus_events (
            id BIGSERIAL PRIMARY KEY,
            telegram_id BIGINT NOT NULL,
            amount BIGINT NOT NULL,
            event_type TEXT NOT NULL,
            reason TEXT,
            expires_at TIMESTAMPTZ,
            is_active BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            consumed_at TIMESTAMPTZ,
            meta TEXT
        );
        """))

        await conn.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_bonus_events_tg_type_active
        ON bonus_events(telegram_id, event_type, is_active);
        """))


async def ensure_user_exists(tg_id: int, username: str | None):
    async with SessionLocal() as session:
        await session.execute(text("""
        INSERT INTO users (telegram_id, username)
        VALUES (:tg, :username)
        ON CONFLICT (telegram_id) DO NOTHING
        """), {"tg": tg_id, "username": username})
        await session.execute(text("""
        UPDATE users
        SET username = COALESCE(:username, username)
        WHERE telegram_id = :tg
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


async def get_stats() -> dict:
    async with SessionLocal() as session:
        today_users = await session.execute(text("""
            SELECT COUNT(*)
            FROM users
            WHERE telegram_id IS NOT NULL
              AND EXISTS (
                  SELECT 1
                  FROM receipts r
                  WHERE r.telegram_id = users.telegram_id
                    AND r.created_at >= date_trunc('day', NOW())
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM receipts r2
                  WHERE r2.telegram_id = users.telegram_id
                    AND r2.created_at < date_trunc('day', NOW())
              )
        """))

        today_receipts = await session.execute(text("""
            SELECT COUNT(*) FROM receipts
            WHERE created_at >= date_trunc('day', NOW())
        """))

        today_amount = await session.execute(text("""
            SELECT COALESCE(SUM(amount), 0) FROM receipts
            WHERE created_at >= date_trunc('day', NOW())
        """))

        today_spent = await session.execute(text("""
            SELECT COALESCE(SUM(amount), 0) FROM redemptions
            WHERE status='used' AND used_at >= date_trunc('day', NOW())
        """))

        today_cashback = await session.execute(text("""
            SELECT COALESCE(SUM(FLOOR(amount * 0.05)), 0) FROM receipts
            WHERE created_at >= date_trunc('day', NOW())
        """))

        returning_users = await session.execute(text("""
            SELECT COUNT(DISTINCT telegram_id)
            FROM receipts
            WHERE created_at >= date_trunc('day', NOW())
        """))

        all_users = await session.execute(text("SELECT COUNT(*) FROM users"))
        all_receipts = await session.execute(text("SELECT COUNT(*) FROM receipts"))
        all_amount = await session.execute(text("SELECT COALESCE(SUM(amount), 0) FROM receipts"))
        all_spent = await session.execute(text("""
            SELECT COALESCE(SUM(amount), 0) FROM redemptions
            WHERE status='used'
        """))
        all_cashback = await session.execute(text("""
            SELECT COALESCE(SUM(FLOOR(amount * 0.05)), 0) FROM receipts
        """))

        return {
            "today_users": int(today_users.scalar() or 0),
            "today_receipts": int(today_receipts.scalar() or 0),
            "today_amount": int(today_amount.scalar() or 0),
            "today_spent": int(today_spent.scalar() or 0),
            "today_cashback": int(today_cashback.scalar() or 0),
            "returning_users": int(returning_users.scalar() or 0),
            "all_users": int(all_users.scalar() or 0),
            "all_receipts": int(all_receipts.scalar() or 0),
            "all_amount": int(all_amount.scalar() or 0),
            "all_spent": int(all_spent.scalar() or 0),
            "all_cashback": int(all_cashback.scalar() or 0),
        }


async def find_client_card(query_text: str) -> dict | None:
    query_text = (query_text or "").strip()
    if not query_text:
        return None

    username = None
    telegram_id = None

    if query_text.startswith("@"):
        username = query_text[1:].strip().lower()
    elif query_text.isdigit():
        telegram_id = int(query_text)
    else:
        username = query_text.strip().lower()

    async with SessionLocal() as session:
        if telegram_id is not None:
            r = await session.execute(text("""
                SELECT telegram_id, username, balance, referrer, last_visit, status
                FROM users
                WHERE telegram_id = :tg
                LIMIT 1
            """), {"tg": telegram_id})
        else:
            r = await session.execute(text("""
                SELECT telegram_id, username, balance, referrer, last_visit, status
                FROM users
                WHERE LOWER(COALESCE(username, '')) = :username
                LIMIT 1
            """), {"username": username})

        row = r.first()
        if not row:
            return None

        tg_id, uname, balance, referrer, last_visit, status = row

        receipts_count = await session.execute(text("""
            SELECT COUNT(*) FROM receipts WHERE telegram_id=:tg
        """), {"tg": tg_id})

        receipts_amount = await session.execute(text("""
            SELECT COALESCE(SUM(amount), 0) FROM receipts WHERE telegram_id=:tg
        """), {"tg": tg_id})

        spent_amount = await session.execute(text("""
            SELECT COALESCE(SUM(amount), 0)
            FROM redemptions
            WHERE telegram_id=:tg AND status='used'
        """), {"tg": tg_id})

        referrer_name = "—"
        if referrer:
            rr = await session.execute(text("""
                SELECT telegram_id, username FROM users WHERE telegram_id=:tg LIMIT 1
            """), {"tg": referrer})
            ref_row = rr.first()
            if ref_row:
                ref_tg, ref_uname = ref_row
                referrer_name = f"@{ref_uname}" if ref_uname else str(ref_tg)
            else:
                referrer_name = str(referrer)

        return {
            "telegram_id": tg_id,
            "username": f"@{uname}" if uname else "—",
            "balance": int(balance or 0),
            "status": status or "Explorer",
            "receipts_count": int(receipts_count.scalar() or 0),
            "receipts_amount": int(receipts_amount.scalar() or 0),
            "spent_amount": int(spent_amount.scalar() or 0),
            "last_visit": format_dt_local(last_visit),
            "referrer": referrer_name,
        }


async def get_recent_receipts(limit: int = 10) -> list[dict]:
    async with SessionLocal() as session:
        r = await session.execute(text("""
            SELECT r.created_at, r.transaction_id, r.amount, r.telegram_id, u.username
            FROM receipts r
            LEFT JOIN users u ON u.telegram_id = r.telegram_id
            ORDER BY r.created_at DESC
            LIMIT :limit
        """), {"limit": limit})
        rows = r.fetchall()

        result = []
        for created_at, transaction_id, amount, telegram_id, username in rows:
            result.append({
                "created_at": created_at,
                "transaction_id": transaction_id,
                "amount": int(amount or 0),
                "telegram_id": telegram_id,
                "username": f"@{username}" if username else "—",
            })
        return result


async def get_recent_redemptions(limit: int = 10) -> list[dict]:
    async with SessionLocal() as session:
        r = await session.execute(text("""
            SELECT rd.created_at, rd.code, rd.amount, rd.status, rd.telegram_id, u.username
            FROM redemptions rd
            LEFT JOIN users u ON u.telegram_id = rd.telegram_id
            ORDER BY rd.created_at DESC
            LIMIT :limit
        """), {"limit": limit})
        rows = r.fetchall()

        result = []
        for created_at, code, amount, status, telegram_id, username in rows:
            result.append({
                "created_at": created_at,
                "code": code,
                "amount": int(amount or 0),
                "status": status or "—",
                "telegram_id": telegram_id,
                "username": f"@{username}" if username else "—",
            })
        return result


async def refresh_user_status(session, tg_id: int) -> tuple[str, int | None, bool]:
    r = await session.execute(text("SELECT COUNT(*) FROM receipts WHERE telegram_id=:tg"), {"tg": tg_id})
    count = int(r.scalar() or 0)
    new_status, remaining = get_status_by_receipts(count)
    cur = await session.execute(text("SELECT status FROM users WHERE telegram_id=:tg"), {"tg": tg_id})
    old_status = cur.scalar() or "Explorer"
    upgraded = old_status != new_status
    await session.execute(text("UPDATE users SET status=:status WHERE telegram_id=:tg"), {"status": new_status, "tg": tg_id})
    return new_status, remaining, upgraded


async def has_active_event(session, tg_id: int, event_type: str) -> bool:
    r = await session.execute(text("""
        SELECT 1
        FROM bonus_events
        WHERE telegram_id = :tg
          AND event_type = :event_type
          AND is_active = TRUE
        LIMIT 1
    """), {"tg": tg_id, "event_type": event_type})
    return r.first() is not None


async def event_exists_with_meta_today(session, event_type: str, meta: str) -> bool:
    r = await session.execute(text("""
        SELECT 1
        FROM bonus_events
        WHERE event_type = :event_type
          AND meta = :meta
        LIMIT 1
    """), {"event_type": event_type, "meta": meta})
    return r.first() is not None


async def give_temporary_bonus(session, tg_id: int, amount: int, event_type: str, reason: str, meta: str | None = None):
    expires_at = datetime.now(timezone.utc) + timedelta(hours=TEMP_BONUS_HOURS)
    await session.execute(text("""
        INSERT INTO bonus_events (telegram_id, amount, event_type, reason, expires_at, is_active, meta)
        VALUES (:tg, :amount, :event_type, :reason, :expires_at, TRUE, :meta)
    """), {
        "tg": tg_id,
        "amount": amount,
        "event_type": event_type,
        "reason": reason,
        "expires_at": expires_at,
        "meta": meta,
    })
    await session.execute(text("""
        UPDATE users
        SET balance = balance + :amount
        WHERE telegram_id = :tg
    """), {"tg": tg_id, "amount": amount})


# =========================
# JOBS
# =========================
async def expire_bonus_events_job(context: ContextTypes.DEFAULT_TYPE):
    async with SessionLocal() as session:
        r = await session.execute(text("""
            SELECT id, telegram_id, amount
            FROM bonus_events
            WHERE is_active = TRUE
              AND expires_at IS NOT NULL
              AND expires_at <= NOW()
        """))
        rows = r.fetchall()

        for bonus_id, telegram_id, amount in rows:
            bal_row = await session.execute(text("SELECT balance FROM users WHERE telegram_id=:tg"), {"tg": telegram_id})
            current_balance = int(bal_row.scalar() or 0)
            new_balance = current_balance - int(amount or 0)
            if new_balance < 0:
                new_balance = 0

            await session.execute(text("UPDATE users SET balance=:bal WHERE telegram_id=:tg"), {"bal": new_balance, "tg": telegram_id})
            await session.execute(text("""
                UPDATE bonus_events
                SET is_active = FALSE,
                    consumed_at = NOW()
                WHERE id = :id
            """), {"id": bonus_id})

        await session.commit()


async def sleep_bonus_job(context: ContextTypes.DEFAULT_TYPE):
    async with SessionLocal() as session:
        rows = await session.execute(text("""
            SELECT telegram_id, lang
            FROM users
            WHERE last_visit IS NOT NULL
              AND last_visit < NOW() - (:days || ' days')::interval
              AND (
                    last_retention_sent_at IS NULL
                    OR last_retention_sent_at < NOW() - (:cooldown || ' days')::interval
                  )
        """), {"days": SLEEP_DAYS, "cooldown": SLEEP_BONUS_COOLDOWN_DAYS})
        users = rows.fetchall()

        for telegram_id, lang in users:
            if await has_active_event(session, telegram_id, "sleep_bonus"):
                continue

            try:
                await give_temporary_bonus(
                    session=session,
                    tg_id=telegram_id,
                    amount=TEMP_BONUS_AMOUNT,
                    event_type="sleep_bonus",
                    reason="sleep_7_days",
                )
                await session.execute(text("""
                    UPDATE users
                    SET last_retention_sent_at = NOW()
                    WHERE telegram_id = :tg
                """), {"tg": telegram_id})
                await context.bot.send_message(
                    chat_id=telegram_id,
                    text=tr(lang or "ru", "sleep_bonus_message")
                )
            except Exception as e:
                print("SLEEP BONUS SEND ERROR:", e)

        await session.commit()


async def first_return_bonus_job(context: ContextTypes.DEFAULT_TYPE):
    async with SessionLocal() as session:
        rows = await session.execute(text("""
            SELECT u.telegram_id, u.lang, MIN(r.created_at) AS first_receipt_at, COUNT(r.id) AS cnt
            FROM users u
            JOIN receipts r ON r.telegram_id = u.telegram_id
            GROUP BY u.telegram_id, u.lang
            HAVING COUNT(r.id) = 1
               AND MIN(r.created_at) <= NOW() - (:days || ' days')::interval
        """), {"days": FIRST_RETURN_AFTER_DAYS})
        users = rows.fetchall()

        for telegram_id, lang, first_receipt_at, cnt in users:
            if await has_active_event(session, telegram_id, "first_return_bonus"):
                continue

            already_given = await session.execute(text("""
                SELECT 1
                FROM bonus_events
                WHERE telegram_id = :tg
                  AND event_type = 'first_return_bonus'
                LIMIT 1
            """), {"tg": telegram_id})
            if already_given.first():
                continue

            try:
                await give_temporary_bonus(
                    session=session,
                    tg_id=telegram_id,
                    amount=TEMP_BONUS_AMOUNT,
                    event_type="first_return_bonus",
                    reason="after_first_receipt_2_days",
                )
                await context.bot.send_message(
                    chat_id=telegram_id,
                    text=tr(lang or "ru", "first_return_bonus_message")
                )
            except Exception as e:
                print("FIRST RETURN BONUS SEND ERROR:", e)

        await session.commit()


async def status_reminder_job(context: ContextTypes.DEFAULT_TYPE):
    async with SessionLocal() as session:
        rows = await session.execute(text("""
            SELECT u.telegram_id, u.lang, COUNT(r.id) AS cnt
            FROM users u
            JOIN receipts r ON r.telegram_id = u.telegram_id
            GROUP BY u.telegram_id, u.lang
        """))
        users = rows.fetchall()

        for telegram_id, lang, cnt in users:
            count = int(cnt or 0)
            next_status, remaining = get_next_status_info(count)
            if not next_status or remaining is None:
                continue
            if remaining > STATUS_REMINDER_THRESHOLD:
                continue

            meta = f"{next_status}:{count}:{datetime.now(LOCAL_TZ).strftime('%Y-%m-%d')}"
            if await event_exists_with_meta_today(session, "status_reminder", meta):
                continue

            recent_same = await session.execute(text("""
                SELECT 1
                FROM bonus_events
                WHERE telegram_id = :tg
                  AND event_type = 'status_reminder'
                  AND reason = :reason
                  AND created_at >= NOW() - (:days || ' days')::interval
                LIMIT 1
            """), {
                "tg": telegram_id,
                "reason": next_status,
                "days": STATUS_REMINDER_COOLDOWN_DAYS,
            })
            if recent_same.first():
                continue

            try:
                await session.execute(text("""
                    INSERT INTO bonus_events (telegram_id, amount, event_type, reason, expires_at, is_active, meta)
                    VALUES (:tg, 0, 'status_reminder', :reason, NULL, FALSE, :meta)
                """), {"tg": telegram_id, "reason": next_status, "meta": meta})
                await context.bot.send_message(
                    chat_id=telegram_id,
                    text=tr(lang or "ru", "status_reminder_message", status=next_status, remaining=remaining)
                )
            except Exception as e:
                print("STATUS REMINDER SEND ERROR:", e)

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

    print("POSTER SUM RAW:", raw)
    print("POSTER SUM TRANSACTION:", transaction)

    try:
        raw = float(raw)
    except Exception as e:
        print("POSTER SUM PARSE ERROR:", e)
        raw = 0.0

    if raw >= 100000:
        total = int(round(raw / 100))
        print("POSTER SUM PARSED AS /100:", total)
        return total

    total = int(round(raw))
    print("POSTER SUM PARSED DIRECT:", total)
    return total


def extract_poster_time(transaction: dict) -> datetime | None:
    keys = [
        "date_close",
        "date_close_date",
        "date_start",
        "date_start_new",
        "date",
        "created_at",
        "closed_at",
        "time",
        "open_date",
        "close_date",
    ]

    print("POSTER TIME RAW TRANSACTION:", transaction)

    for k in keys:
        v = transaction.get(k)
        print(f"POSTER TIME CANDIDATE {k} =", repr(v))

        if v is None or v == "":
            continue

        if isinstance(v, str) and v.strip().isdigit():
            try:
                v_num = float(v.strip())
            except Exception:
                v_num = None
        else:
            v_num = v if isinstance(v, (int, float)) else None

        if v_num is not None and 1000000000 <= float(v_num) < 1000000000000:
            try:
                dt = datetime.fromtimestamp(float(v_num), tz=timezone.utc)
                print(f"PARSED {k} AS UNIX SECONDS ->", dt.isoformat())
                return dt
            except Exception as e:
                print(f"FAILED PARSE {k} AS UNIX SECONDS:", e)

        if v_num is not None and float(v_num) >= 1000000000000:
            try:
                dt = datetime.fromtimestamp(float(v_num) / 1000, tz=timezone.utc)
                print(f"PARSED {k} AS UNIX MILLISECONDS ->", dt.isoformat())
                return dt
            except Exception as e:
                print(f"FAILED PARSE {k} AS UNIX MILLISECONDS:", e)

        if isinstance(v, str):
            s = v.strip()

            if s.endswith("Z"):
                try:
                    dt = datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)
                    print(f"PARSED {k} AS ISO Z ->", dt.isoformat())
                    return dt
                except Exception as e:
                    print(f"FAILED PARSE {k} AS ISO Z:", e)

            try:
                dt = datetime.fromisoformat(s)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=LOCAL_TZ)
                dt = dt.astimezone(timezone.utc)
                print(f"PARSED {k} AS ISO ->", dt.isoformat())
                return dt
            except Exception:
                pass

            fmts = [
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%dT%H:%M:%S.%f",
                "%d.%m.%Y %H:%M:%S",
                "%d.%m.%Y %H:%M",
                "%Y-%m-%d %H:%M:%S %z",
                "%Y-%m-%d %H:%M %z",
            ]
            for fmt in fmts:
                try:
                    dt = datetime.strptime(s, fmt)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=LOCAL_TZ)
                    dt = dt.astimezone(timezone.utc)
                    print(f"PARSED {k} WITH FORMAT {fmt} ->", dt.isoformat())
                    return dt
                except Exception:
                    continue

    print("POSTER TIME PARSE FAILED: no usable datetime found")
    return None


def is_receipt_too_old(poster_time: datetime | None) -> bool:
    if poster_time is None:
        print("TIME CHECK: poster_time is None -> treat as OLD")
        return True

    now_utc = datetime.now(timezone.utc)
    diff = now_utc - poster_time

    print("TIME CHECK NOW UTC:", now_utc.isoformat())
    print("TIME CHECK POSTER UTC:", poster_time.isoformat())
    print("TIME CHECK DIFF MINUTES:", diff.total_seconds() / 60)

    return diff > timedelta(minutes=RECEIPT_TTL_MINUTES)


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


# =========================
# КНОПКИ / МЕНЮ
# =========================
def main_menu_keyboard(tg_id: int, lang: str) -> InlineKeyboardMarkup:
    if is_admin(tg_id):
        rows = [
            [InlineKeyboardButton(tr(lang, "menu_balance"), callback_data="menu:balance")],
            [InlineKeyboardButton(tr(lang, "menu_earn"), callback_data="menu:earn")],
            [InlineKeyboardButton(tr(lang, "menu_spend"), callback_data="menu:spend")],
            [InlineKeyboardButton(tr(lang, "menu_invite"), callback_data="menu:invite")],
            [InlineKeyboardButton(tr(lang, "menu_status"), callback_data="menu:status")],
            [InlineKeyboardButton(tr(lang, "menu_confirm"), callback_data="menu:confirm")],
            [InlineKeyboardButton(tr(lang, "menu_stats"), callback_data="menu:stats")],
            [InlineKeyboardButton(tr(lang, "menu_client"), callback_data="menu:client")],
            [InlineKeyboardButton(tr(lang, "menu_recent_receipts"), callback_data="menu:recent_receipts")],
            [InlineKeyboardButton(tr(lang, "menu_recent_redemptions"), callback_data="menu:recent_redemptions")],
            [InlineKeyboardButton(tr(lang, "menu_adjust"), callback_data="menu:adjust")],
            [InlineKeyboardButton(tr(lang, "menu_language"), callback_data="menu:language")],
            [InlineKeyboardButton(tr(lang, "menu_cancel"), callback_data="menu:cancel")],
        ]
        return InlineKeyboardMarkup(rows)

    if is_staff(tg_id):
        rows = [
            [InlineKeyboardButton(tr(lang, "menu_confirm"), callback_data="menu:confirm")],
            [InlineKeyboardButton(tr(lang, "menu_stats"), callback_data="menu:stats")],
            [InlineKeyboardButton(tr(lang, "menu_client"), callback_data="menu:client")],
            [InlineKeyboardButton(tr(lang, "menu_recent_receipts"), callback_data="menu:recent_receipts")],
            [InlineKeyboardButton(tr(lang, "menu_recent_redemptions"), callback_data="menu:recent_redemptions")],
            [InlineKeyboardButton(tr(lang, "menu_language"), callback_data="menu:language")],
            [InlineKeyboardButton(tr(lang, "menu_cancel"), callback_data="menu:cancel")],
        ]
        return InlineKeyboardMarkup(rows)

    rows = [
        [InlineKeyboardButton(tr(lang, "menu_balance"), callback_data="menu:balance")],
        [InlineKeyboardButton(tr(lang, "menu_earn"), callback_data="menu:earn")],
        [InlineKeyboardButton(tr(lang, "menu_spend"), callback_data="menu:spend")],
        [InlineKeyboardButton(tr(lang, "menu_invite"), callback_data="menu:invite")],
        [InlineKeyboardButton(tr(lang, "menu_status"), callback_data="menu:status")],
        [InlineKeyboardButton(tr(lang, "menu_language"), callback_data="menu:language")],
        [InlineKeyboardButton(tr(lang, "menu_cancel"), callback_data="menu:cancel")],
    ]
    return InlineKeyboardMarkup(rows)


def language_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[ 
        InlineKeyboardButton("Русский", callback_data="lang:ru"),
        InlineKeyboardButton("Қазақша", callback_data="lang:kk"),
    ]])


async def show_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, text_msg: str | None = None):
    user = update.effective_user
    lang = await get_user_lang(user.id)
    kb = main_menu_keyboard(user.id, lang)
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
            raw = str(context.args[0]).replace("ref_", "")
            ref = int(raw)
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
    lang = "kk" if query.data == "lang:kk" else "ru"

    await set_user_lang(tg_id, lang)
    saved_msg = "language_saved_ru" if lang == "ru" else "language_saved_kk"

    await query.message.reply_text(tr(lang, saved_msg))
    await query.message.reply_text(
        tr(lang, "main_menu"),
        reply_markup=main_menu_keyboard(tg_id, lang)
    )


async def on_menu_click(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    tg_id = query.from_user.id
    username = query.from_user.username
    await ensure_user_exists(tg_id, username)
    lang = await get_user_lang(tg_id)

    action = query.data or ""

    if action == "menu:cancel":
        context.user_data.clear()
        await query.message.reply_text(
            tr(lang, "reset_done"),
            reply_markup=main_menu_keyboard(tg_id, lang)
        )
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
            reply_markup=main_menu_keyboard(tg_id, lang)
        )
        return

    if action == "menu:invite":
        bot_username = (await context.bot.get_me()).username
        link = f"https://t.me/{bot_username}?start={tg_id}"
        await query.message.reply_text(
            tr(lang, "invite_text", link=link, friend_bonus=FRIEND_BONUS),
            reply_markup=main_menu_keyboard(tg_id, lang)
        )
        return

    if action == "menu:language":
        await query.message.reply_text(
            tr(lang, "choose_language"),
            reply_markup=language_keyboard()
        )
        return

    if action == "menu:status":
        async with SessionLocal() as session:
            r = await session.execute(text("SELECT COUNT(*) FROM receipts WHERE telegram_id=:tg"), {"tg": tg_id})
            count = int(r.scalar() or 0)
        status, remaining = get_status_by_receipts(count)
        rem_text = "максимум" if lang == "ru" and remaining is None else "макс" if remaining is None else str(remaining)
        if lang == "kk" and remaining is None:
            rem_text = "макс"
        await query.message.reply_text(
            tr(lang, "status_text", status=status, count=count, remaining=rem_text),
            reply_markup=main_menu_keyboard(tg_id, lang)
        )
        return

    if action == "menu:stats":
        if not is_staff(tg_id):
            await query.message.reply_text(tr(lang, "stats_staff_only"))
            return
        stats = await get_stats()
        await query.message.reply_text(
            tr(lang, "stats_text", **stats),
            reply_markup=main_menu_keyboard(tg_id, lang)
        )
        return

    if action == "menu:client":
        if not is_staff(tg_id):
            await query.message.reply_text(tr(lang, "client_staff_only"))
            return
        context.user_data.clear()
        context.user_data["mode"] = "staff_wait_client_query"
        await query.message.reply_text(
            tr(lang, "client_enter_query"),
            reply_markup=main_menu_keyboard(tg_id, lang)
        )
        return

    if action == "menu:recent_receipts":
        if not is_staff(tg_id):
            await query.message.reply_text(tr(lang, "recent_receipts_staff_only"))
            return
        items = await get_recent_receipts()
        if not items:
            await query.message.reply_text(
                tr(lang, "recent_receipts_empty"),
                reply_markup=main_menu_keyboard(tg_id, lang)
            )
            return

        lines = []
        for item in items:
            created_at = item["created_at"]
            time_str = created_at.astimezone(LOCAL_TZ).strftime("%d.%m %H:%M") if created_at else "—"
            lines.append(
                f"{time_str} | чек {item['transaction_id']} | {item['amount']} ₸ | {item['username']} | {item['telegram_id']}"
            )

        await query.message.reply_text(
            tr(lang, "recent_receipts_title", items="\n".join(lines)),
            reply_markup=main_menu_keyboard(tg_id, lang)
        )
        return

    if action == "menu:recent_redemptions":
        if not is_staff(tg_id):
            await query.message.reply_text(tr(lang, "recent_redemptions_staff_only"))
            return
        items = await get_recent_redemptions()
        if not items:
            await query.message.reply_text(
                tr(lang, "recent_redemptions_empty"),
                reply_markup=main_menu_keyboard(tg_id, lang)
            )
            return

        lines = []
        for item in items:
            created_at = item["created_at"]
            time_str = created_at.astimezone(LOCAL_TZ).strftime("%d.%m %H:%M") if created_at else "—"
            lines.append(
                f"{time_str} | код {item['code']} | {item['amount']} ₸ | {item['status']} | {item['username']} | {item['telegram_id']}"
            )

        await query.message.reply_text(
            tr(lang, "recent_redemptions_title", items="\n".join(lines)),
            reply_markup=main_menu_keyboard(tg_id, lang)
        )
        return

    if action == "menu:earn":
        context.user_data.clear()
        context.user_data["mode"] = "earn_wait_receipt"
        await query.message.reply_text(
            tr(lang, "enter_receipt"),
            reply_markup=main_menu_keyboard(tg_id, lang)
        )
        return

    if action == "menu:spend":
        context.user_data.clear()
        context.user_data["mode"] = "spend_wait_amount"
        await query.message.reply_text(
            tr(lang, "spend_enter_amount"),
            reply_markup=main_menu_keyboard(tg_id, lang)
        )
        return

    if action == "menu:confirm":
        if not is_staff(tg_id):
            await query.message.reply_text(tr(lang, "confirm_staff_only"))
            return
        context.user_data.clear()
        context.user_data["mode"] = "confirm_wait_code"
        await query.message.reply_text(
            tr(lang, "confirm_enter_code"),
            reply_markup=main_menu_keyboard(tg_id, lang)
        )
        return

    if action == "menu:adjust":
        if not is_admin(tg_id):
            await query.message.reply_text(tr(lang, "adjust_admin_only"))
            return
        context.user_data.clear()
        context.user_data["mode"] = "adjust_wait_client"
        await query.message.reply_text(
            tr(lang, "adjust_enter_client"),
            reply_markup=main_menu_keyboard(tg_id, lang)
        )
        return

    await query.message.reply_text(tr(lang, "unknown_action"))


# =========================
# ТЕКСТ
# =========================
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tg_id = update.effective_user.id
    username = update.effective_user.username
    await ensure_user_exists(tg_id, username)
    lang = await get_user_lang(tg_id)

    text_msg = (update.message.text or "").strip()
    mode = context.user_data.get("mode")

    # 0. Сотрудник ищет клиента
    if mode == "staff_wait_client_query":
        if not is_staff(tg_id):
            context.user_data.clear()
            await update.message.reply_text(tr(lang, "client_staff_only"))
            return

        card = await find_client_card(text_msg)
        context.user_data.clear()

        if not card:
            await update.message.reply_text(
                tr(lang, "client_not_found"),
                reply_markup=main_menu_keyboard(tg_id, lang)
            )
            return

        await update.message.reply_text(
            tr(lang, "client_card", **card),
            reply_markup=main_menu_keyboard(tg_id, lang)
        )
        return

    # 0.1 Админ: поиск клиента для корректировки
    if mode == "adjust_wait_client":
        if not is_admin(tg_id):
            context.user_data.clear()
            await update.message.reply_text(tr(lang, "adjust_admin_only"))
            return

        card = await find_client_card(text_msg)
        if not card:
            await update.message.reply_text(tr(lang, "client_not_found"))
            return

        context.user_data["adjust_client_id"] = card["telegram_id"]
        context.user_data["adjust_client_name"] = card["username"] if card["username"] != "—" else str(card["telegram_id"])
        context.user_data["mode"] = "adjust_wait_delta"
        await update.message.reply_text(tr(lang, "adjust_enter_delta"))
        return

    if mode == "adjust_wait_delta":
        if not re.fullmatch(r"[+-]?\d+", text_msg or ""):
            await update.message.reply_text(tr(lang, "adjust_enter_delta"))
            return
        delta = int(text_msg)
        if delta == 0:
            await update.message.reply_text(tr(lang, "adjust_enter_delta"))
            return
        context.user_data["adjust_delta"] = delta
        context.user_data["mode"] = "adjust_wait_reason"
        await update.message.reply_text(tr(lang, "adjust_enter_reason"))
        return

    if mode == "adjust_wait_reason":
        reason = text_msg or "ручная корректировка"
        client_id = context.user_data.get("adjust_client_id")
        delta = int(context.user_data.get("adjust_delta") or 0)
        client_label = context.user_data.get("adjust_client_name") or str(client_id)

        async with SessionLocal() as session:
            r = await session.execute(text("SELECT balance, lang FROM users WHERE telegram_id=:tg"), {"tg": client_id})
            row = r.first()
            if not row:
                context.user_data.clear()
                await update.message.reply_text(tr(lang, "client_not_found"))
                return

            current_balance, client_lang = row
            current_balance = int(current_balance or 0)
            new_balance = current_balance + delta
            real_delta = delta
            if new_balance < 0:
                real_delta = -current_balance
                new_balance = 0

            await session.execute(text("UPDATE users SET balance=:bal WHERE telegram_id=:tg"), {"bal": new_balance, "tg": client_id})
            await session.execute(text("""
                INSERT INTO balance_adjustments (target_telegram_id, admin_telegram_id, delta, reason)
                VALUES (:target, :admin, :delta, :reason)
            """), {"target": client_id, "admin": tg_id, "delta": real_delta, "reason": reason})
            await session.commit()

        context.user_data.clear()
        await update.message.reply_text(
            tr(lang, "adjust_done", user=client_label, delta=real_delta, balance=new_balance, reason=reason),
            reply_markup=main_menu_keyboard(tg_id, lang)
        )
        try:
            await context.bot.send_message(
                chat_id=client_id,
                text=tr(client_lang or "ru", "adjust_notice", delta=real_delta, balance=new_balance, reason=reason)
            )
        except Exception as e:
            print("ADJUST NOTICE ERROR:", e)
        return

    # 1. Ждём номер чека
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
            check = await session.execute(
                text("SELECT id FROM receipts WHERE transaction_id=:tid"),
                {"tid": receipt}
            )
            if check.first():
                await update.message.reply_text(tr(lang, "receipt_already_used"))
                return

            if not is_admin(tg_id):
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

    # 2. Ждём сумму
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

        print("SUM CHECK USER INPUT:", amount)
        print("SUM CHECK EXPECTED:", expected)
        print("SUM CHECK DIFF:", abs(amount - expected))

        if abs(amount - expected) > AMOUNT_TOLERANCE_TENGE:
            await update.message.reply_text(tr(lang, "amount_mismatch"))
            return

        if not is_admin(tg_id) and poster_time is None:
            context.user_data.clear()
            await update.message.reply_text(tr(lang, "cannot_check_time"))
            return

        if not is_admin(tg_id) and is_receipt_too_old(poster_time):
            context.user_data.clear()
            await update.message.reply_text(tr(lang, "receipt_too_old"))
            return

        cashback = int(expected * 0.05)
        status_upgraded_to = None

        async with SessionLocal() as session:
            check = await session.execute(
                text("SELECT id FROM receipts WHERE transaction_id=:tid"),
                {"tid": receipt_id}
            )
            if check.first():
                context.user_data.clear()
                await update.message.reply_text(tr(lang, "receipt_already_used"))
                return

            if not is_admin(tg_id):
                today = await session.execute(text("""
                    SELECT COUNT(*) FROM receipts
                    WHERE telegram_id=:tg
                      AND created_at >= date_trunc('day', NOW())
                """), {"tg": tg_id})
                if (today.scalar() or 0) >= DAILY_LIMIT:
                    context.user_data.clear()
                    await update.message.reply_text(tr(lang, "daily_limit"))
                    return

            prev = await session.execute(
                text("SELECT COUNT(*) FROM receipts WHERE telegram_id=:tg"),
                {"tg": tg_id}
            )
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
                r2 = await session.execute(
                    text("SELECT referrer FROM users WHERE telegram_id=:tg"),
                    {"tg": ref1}
                )
                ref2 = r2.scalar()

            if ref2:
                r3 = await session.execute(
                    text("SELECT referrer FROM users WHERE telegram_id=:tg"),
                    {"tg": ref2}
                )
                ref3 = r3.scalar()

            await session.execute(text("""
                INSERT INTO receipts (transaction_id, telegram_id, amount, poster_time)
                VALUES (:tid, :tg, :amount, :poster_time)
            """), {
                "tid": receipt_id,
                "tg": tg_id,
                "amount": expected,
                "poster_time": poster_time
            })

            await session.execute(
                text("""
                    UPDATE users
                    SET balance = balance + :b,
                        last_visit = NOW()
                    WHERE telegram_id=:tg
                """),
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

            new_status, _, upgraded = await refresh_user_status(session, tg_id)
            if upgraded:
                status_upgraded_to = new_status

            await session.commit()

            rbal = await session.execute(
                text("SELECT balance FROM users WHERE telegram_id=:tg"),
                {"tg": tg_id}
            )
            new_balance = rbal.scalar() or 0

        context.user_data.clear()
        await update.message.reply_text(
            tr(lang, "done", receipt=receipt_id, amount=expected, cashback=cashback, balance=new_balance)
        )
        if status_upgraded_to:
            await update.message.reply_text(tr(lang, "status_upgraded", status=status_upgraded_to))
        await show_menu(update, context, tr(lang, "choose_action"))
        return

    # 3. Списание
    if mode == "spend_wait_amount":
        amount = parse_amount_tenge(text_msg)
        if amount is None or amount <= 0:
            await update.message.reply_text(tr(lang, "amount_not_understood"))
            return

        async with SessionLocal() as session:
            r = await session.execute(
                text("SELECT balance FROM users WHERE telegram_id=:tg"),
                {"tg": tg_id}
            )
            bal = r.scalar() or 0

            if bal < amount:
                context.user_data.clear()
                await update.message.reply_text(tr(lang, "not_enough_points", amount=amount, balance=bal))
                await show_menu(update, context, tr(lang, "choose_action"))
                return

            code = generate_code()
            for _ in range(3):
                try:
                    await session.execute(text("""
                        INSERT INTO redemptions (code, telegram_id, amount, status)
                        VALUES (:code, :tg, :amt, 'pending')
                    """), {"code": code, "tg": tg_id, "amt": amount})
                    await session.commit()
                    break
                except Exception:
                    await session.rollback()
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

    # 4. Подтверждение кода
    if mode == "confirm_wait_code":
        if not is_staff(tg_id):
            context.user_data.clear()
            await update.message.reply_text(tr(lang, "confirm_staff_only"))
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
                await session.execute(
                    text("UPDATE redemptions SET status='expired' WHERE id=:id"),
                    {"id": rid}
                )
                await session.commit()
                await update.message.reply_text(tr(lang, "confirm_code_expired"))
                return

            r2 = await session.execute(
                text("SELECT balance, lang FROM users WHERE telegram_id=:tg"),
                {"tg": user_id}
            )
            user_row = r2.first()
            bal = user_row[0] if user_row else 0
            client_lang = user_row[1] if user_row else "ru"

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
        try:
            async with SessionLocal() as session:
                rb = await session.execute(text("SELECT balance FROM users WHERE telegram_id=:tg"), {"tg": user_id})
                new_balance = int(rb.scalar() or 0)
            await context.bot.send_message(
                chat_id=user_id,
                text=tr(client_lang or "ru", "balance", balance=new_balance)
            )
        except Exception as e:
            print("CLIENT REDEEM NOTICE ERROR:", e)
        await show_menu(update, context, tr(lang, "choose_action"))
        return

    await show_menu(update, context, tr(lang, "unknown_text"))


async def on_startup(app):
    await create_or_update_tables()

    if app.job_queue:
        # Спящие гости: каждый день в 12:00
        app.job_queue.run_daily(
            sleep_bonus_job,
            time=time(hour=12, minute=0, tzinfo=LOCAL_TZ),
            name="sleep_bonus_job",
        )

        # После первого визита: каждый день в 12:10
        app.job_queue.run_daily(
            first_return_bonus_job,
            time=time(hour=12, minute=10, tzinfo=LOCAL_TZ),
            name="first_return_bonus_job",
        )

        # Напоминание о статусе: каждый день в 12:20
        app.job_queue.run_daily(
            status_reminder_job,
            time=time(hour=12, minute=20, tzinfo=LOCAL_TZ),
            name="status_reminder_job",
        )

        # Сгорание временных бонусов: каждый час
        app.job_queue.run_repeating(
            expire_bonus_events_job,
            interval=3600,
            first=30,
            name="expire_bonus_events_job",
        )


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
