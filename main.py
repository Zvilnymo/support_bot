import re
import os
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    Updater, MessageHandler, Filters, CallbackContext,
    CommandHandler, ConversationHandler, CallbackQueryHandler
)
from collections import Counter
from openpyxl import Workbook
from io import BytesIO

# ==========================================
# НАСТРОЙКИ
# ==========================================
BOT_TOKEN = os.environ["BOT_TOKEN"]
DATABASE_URL = os.environ["DATABASE_URL"]

# Вебхуки Bitrix24
BITRIX_CONTACT_URL = os.environ["BITRIX_CONTACT_URL"]  # crm.contact.list
BITRIX_TASK_URL = os.environ["BITRIX_TASK_URL"]        # task.item.add

# Админы (только для управления сотрудниками/категориями)
ADMIN_TELEGRAM_IDS = [727013047, 458757059, 8183276948]

# ID чатов для отделов
SUPPORT_CHAT_ID = -1003053461710
PRE_TRIAL_CHAT_ID = -1003588501355

# Дефолтный ответственный для новых сотрудников
RESPONSIBLE_ID = 596

# Состояния ConversationHandler (для /add_employee и /add_category)
(
    ADD_EMPLOYEE_TG_ID,
    ADD_EMPLOYEE_BITRIX_ID,
    ADD_EMPLOYEE_NAME,
    ADD_CATEGORY_CODE,
    ADD_CATEGORY_NAME,
) = range(5)

# Состояния рабочего флоу (хранятся в user_data)
STATE_WAITING_CATEGORY = 'waiting_category'
STATE_WAITING_COMMENT   = 'waiting_comment'
STATE_AWAITING_DUPLICATE = 'awaiting_duplicate'

# ==========================================
# POSTGRESQL CONNECTION POOL
# ==========================================
pool = None
categories_cache = {}
categories_cache_time = {}

def init_pool():
    global pool
    if pool is None:
        pool = SimpleConnectionPool(1, 10, DATABASE_URL)
    return pool

def get_conn():
    if pool is None:
        init_pool()
    return pool.getconn()

def release_conn(conn):
    if pool:
        pool.putconn(conn)

def get_department_by_chat_id(chat_id):
    if chat_id == SUPPORT_CHAT_ID:
        return 'support'
    elif chat_id == PRE_TRIAL_CHAT_ID:
        return 'pre_trial'
    return None

def get_table_prefix(department):
    if department in ('support', 'pre_trial'):
        return department
    return None

# ==========================================
# DATABASE FUNCTIONS - EMPLOYEES
# ==========================================

def get_employee_by_telegram_id(telegram_id, department):
    prefix = get_table_prefix(department)
    if not prefix:
        return None
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"SELECT * FROM {prefix}_employees WHERE telegram_id = %s",
                (telegram_id,)
            )
            return cur.fetchone()
    finally:
        release_conn(conn)

def add_employee(telegram_id, name, bitrix_id, department):
    prefix = get_table_prefix(department)
    if not prefix:
        return False
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {prefix}_employees (telegram_id, name, bitrix_id)
                VALUES (%s, %s, %s)
                ON CONFLICT (telegram_id) DO UPDATE
                SET name = EXCLUDED.name, bitrix_id = EXCLUDED.bitrix_id
                """,
                (telegram_id, name, bitrix_id)
            )
            conn.commit()
            return True
    except Exception as e:
        conn.rollback()
        print(f"❌ add_employee error: {e}")
        return False
    finally:
        release_conn(conn)

def delete_employee(telegram_id, department):
    prefix = get_table_prefix(department)
    if not prefix:
        return False
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {prefix}_employees WHERE telegram_id = %s",
                (telegram_id,)
            )
            conn.commit()
            return cur.rowcount > 0
    except Exception as e:
        conn.rollback()
        print(f"❌ delete_employee error: {e}")
        return False
    finally:
        release_conn(conn)

def get_all_employees(department):
    prefix = get_table_prefix(department)
    if not prefix:
        return []
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(f"SELECT * FROM {prefix}_employees ORDER BY name")
            return cur.fetchall()
    finally:
        release_conn(conn)

# ==========================================
# DATABASE FUNCTIONS - CATEGORIES
# ==========================================

def get_category_by_code(code, department):
    prefix = get_table_prefix(department)
    if not prefix:
        return None
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"SELECT * FROM {prefix}_categories WHERE code = %s",
                (code.upper(),)
            )
            return cur.fetchone()
    finally:
        release_conn(conn)

def add_category(code, name, department):
    global categories_cache, categories_cache_time
    prefix = get_table_prefix(department)
    if not prefix:
        return False
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {prefix}_categories (code, name)
                VALUES (%s, %s)
                ON CONFLICT (code) DO UPDATE
                SET name = EXCLUDED.name
                """,
                (code.upper(), name)
            )
            conn.commit()
            categories_cache.pop(department, None)
            categories_cache_time.pop(department, None)
            return True
    except Exception as e:
        conn.rollback()
        print(f"❌ add_category error: {e}")
        return False
    finally:
        release_conn(conn)

def delete_category(code, department):
    global categories_cache, categories_cache_time
    prefix = get_table_prefix(department)
    if not prefix:
        return False
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {prefix}_categories WHERE code = %s",
                (code.upper(),)
            )
            conn.commit()
            categories_cache.pop(department, None)
            categories_cache_time.pop(department, None)
            return cur.rowcount > 0
    except Exception as e:
        conn.rollback()
        print(f"❌ delete_category error: {e}")
        return False
    finally:
        release_conn(conn)

def get_all_categories(department, use_cache=True):
    global categories_cache, categories_cache_time
    prefix = get_table_prefix(department)
    if not prefix:
        return []

    if use_cache and department in categories_cache and department in categories_cache_time:
        if (datetime.now() - categories_cache_time[department]).total_seconds() < 60:
            return categories_cache[department]

    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(f"SELECT * FROM {prefix}_categories ORDER BY code")
            result = cur.fetchall()
            if use_cache:
                categories_cache[department] = result
                categories_cache_time[department] = datetime.now()
            return result
    finally:
        release_conn(conn)

# ==========================================
# DATABASE FUNCTIONS - RECORDS
# ==========================================

def add_record(employee_telegram_id, category_code, phone, comment, department):
    prefix = get_table_prefix(department)
    if not prefix:
        return None
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {prefix}_records
                (employee_telegram_id, category_code, phone, comment)
                VALUES (%s, %s, %s, %s)
                RETURNING id
                """,
                (employee_telegram_id, category_code.upper(), phone, comment)
            )
            conn.commit()
            return cur.fetchone()[0]
    except Exception as e:
        conn.rollback()
        print(f"❌ add_record error: {e}")
        return None
    finally:
        release_conn(conn)

def check_duplicate_record(employee_telegram_id, category_code, phone, department, minutes=5):
    prefix = get_table_prefix(department)
    if not prefix:
        return False
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT COUNT(*) FROM {prefix}_records
                WHERE employee_telegram_id = %s
                AND category_code = %s
                AND phone = %s
                AND timestamp > NOW() - make_interval(mins => %s)
                """,
                (employee_telegram_id, category_code.upper(), phone, minutes)
            )
            return cur.fetchone()[0] > 0
    finally:
        release_conn(conn)

def get_records_by_phone(phone, days, department):
    prefix = get_table_prefix(department)
    if not prefix:
        return []
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT
                    r.timestamp,
                    e.name as employee_name,
                    c.name as category_name,
                    r.category_code,
                    r.phone,
                    r.comment
                FROM {prefix}_records r
                LEFT JOIN {prefix}_employees e ON r.employee_telegram_id = e.telegram_id
                LEFT JOIN {prefix}_categories c ON r.category_code = c.code
                WHERE r.phone = %s
                AND r.timestamp > NOW() - make_interval(days => %s)
                ORDER BY r.timestamp DESC
                """,
                (phone, days)
            )
            return cur.fetchall()
    finally:
        release_conn(conn)

def get_team_stats(days, department):
    prefix = get_table_prefix(department)
    if not prefix:
        return {'total': 0, 'by_employee': [], 'by_category': []}
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT COUNT(*) as total_records
                FROM {prefix}_records
                WHERE timestamp > NOW() - make_interval(days => %s)
                """,
                (days,)
            )
            total = cur.fetchone()['total_records']

            cur.execute(
                f"""
                SELECT e.name, COUNT(*) as count
                FROM {prefix}_records r
                LEFT JOIN {prefix}_employees e ON r.employee_telegram_id = e.telegram_id
                WHERE r.timestamp > NOW() - make_interval(days => %s)
                GROUP BY e.name
                ORDER BY count DESC
                """,
                (days,)
            )
            by_employee = cur.fetchall()

            cur.execute(
                f"""
                SELECT c.name, c.code, COUNT(*) as count
                FROM {prefix}_records r
                LEFT JOIN {prefix}_categories c ON r.category_code = c.code
                WHERE r.timestamp > NOW() - make_interval(days => %s)
                GROUP BY c.name, c.code
                ORDER BY count DESC
                """,
                (days,)
            )
            by_category = cur.fetchall()

            return {'total': total, 'by_employee': by_employee, 'by_category': by_category}
    finally:
        release_conn(conn)

def get_all_records(days, department):
    prefix = get_table_prefix(department)
    if not prefix:
        return []
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT
                    r.timestamp,
                    e.name as employee_name,
                    c.name as category_name,
                    r.category_code,
                    r.phone,
                    r.comment
                FROM {prefix}_records r
                LEFT JOIN {prefix}_employees e ON r.employee_telegram_id = e.telegram_id
                LEFT JOIN {prefix}_categories c ON r.category_code = c.code
                WHERE r.timestamp > NOW() - make_interval(days => %s)
                ORDER BY r.timestamp DESC
                """,
                (days,)
            )
            return cur.fetchall()
    finally:
        release_conn(conn)

# ==========================================
# УТИЛИТЫ
# ==========================================

def clean_phone(p: str) -> str:
    return re.sub(r"\D", "", p)

def normalize_phone(phone: str) -> str:
    digits = clean_phone(phone)
    regular_phone1 = re.match(r'380\d{9}$', digits)
    regular_phone2 = re.match(r'0\d{9}$', digits)
    regular_phone3 = re.match(r'\d{9}$', digits)

    if regular_phone1:
        new_phone = regular_phone1.group()
    elif regular_phone2:
        new_phone = '38' + regular_phone2.group()
    elif regular_phone3:
        new_phone = '380' + regular_phone3.group()
    else:
        new_phone = digits
    return "+" + new_phone

def try_parse_phone(text: str):
    """Returns normalized phone string if text looks like a phone, else None."""
    if not re.match(r'^[\+\d\s\(\)\-]{7,20}$', text.strip()):
        return None
    digits = re.sub(r'\D', '', text)
    if not (9 <= len(digits) <= 13):
        return None
    return normalize_phone(text)

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_TELEGRAM_IDS

# ==========================================
# INLINE KEYBOARDS
# ==========================================

def build_categories_keyboard(categories):
    buttons = [
        InlineKeyboardButton(cat['name'], callback_data=f"cat_{cat['code']}")
        for cat in categories
    ]
    # 2 buttons per row
    keyboard = [buttons[i:i+2] for i in range(0, len(buttons), 2)]
    return InlineKeyboardMarkup(keyboard)

def build_duplicate_keyboard():
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("Так", callback_data="dup_yes"),
        InlineKeyboardButton("Ні",  callback_data="dup_no"),
    ]])

# ==========================================
# BITRIX24 ІНТЕГРАЦІЯ
# ==========================================

def find_contact_by_phone(phone):
    norm_phone_full = normalize_phone(phone)
    try:
        r = requests.get(
            BITRIX_CONTACT_URL,
            params={
                "filter[PHONE]": norm_phone_full,
                "select[]": ["ID", "NAME", "LAST_NAME", "PHONE"]
            }
        )
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"❌ Bitrix24 error: {e}")
        return None

    result = data.get("result", [])
    if not result:
        return None

    for c in result:
        for ph in c.get("PHONE", []):
            if clean_phone(ph.get("VALUE", "")) == clean_phone(norm_phone_full):
                return c
    return None

def create_task(contact_id, category, comment, responsible_id):
    now = datetime.now()
    deadline = now + timedelta(days=1)
    deadline_str = deadline.strftime("%Y-%m-%dT%H:%M:%S+03:00")

    payload = {
        "fields": {
            "TITLE": f"Запис: {category}",
            "DESCRIPTION": comment,
            "RESPONSIBLE_ID": responsible_id,
            "DEADLINE": deadline_str,
            "UF_CRM_TASK": [f"C_{contact_id}"],
        },
        "notify": True
    }

    task_res = requests.post(BITRIX_TASK_URL, json=payload)
    if task_res.status_code != 200:
        print(f"❌ create_task: {task_res.text}")
        return

    task_id = task_res.json().get("result")
    if not task_id:
        print("❌ create_task: no task id")
        return

    comment_url = BITRIX_CONTACT_URL.replace("crm.contact.list", "crm.timeline.comment.add")
    requests.post(comment_url, json={
        "fields": {
            "ENTITY_ID": contact_id,
            "ENTITY_TYPE": "contact",
            "COMMENT": f"📌 {category}: {comment}",
            "AUTHOR_ID": responsible_id
        }
    })

    complete_url = BITRIX_TASK_URL.replace("task.item.add", "task.complete")
    requests.post(complete_url, json={"id": task_id})

# ==========================================
# ЗБЕРЕЖЕННЯ ЗАПИСУ
# ==========================================

def _save_record(employee_telegram_id, code, phone, comment, category_name, employee_name, responsible_id, department):
    """Saves record to DB and Bitrix. Returns message text for the user."""
    contact = find_contact_by_phone(phone)
    if not contact:
        return "❗ Клієнт не знайдений у CRM"

    create_task(contact["ID"], category_name, comment, responsible_id)

    record_id = add_record(employee_telegram_id, code, phone, comment, department)

    if record_id:
        client_name = f"{contact.get('NAME', '')} {contact.get('LAST_NAME', '')}".strip()
        return f"✅ Запис збережено: {category_name} – {client_name}"
    else:
        return "⚠ Помилка збереження у БД, але задача у Bitrix створена"

# ==========================================
# ОСНОВНИЙ ОБРОБНИК ПОВІДОМЛЕНЬ
# ==========================================

def handle_message(update: Update, context: CallbackContext):
    print(f"📨 Отримано повідомлення з чату: {update.message.chat_id}", flush=True)
    print(f"📝 Текст: {update.message.text}", flush=True)

    department = get_department_by_chat_id(update.message.chat_id)
    if not department:
        print("⚠️ Повідомлення з непідтримуваного чату, ігноруємо")
        return

    state = context.user_data.get('state')

    # Якщо чекаємо коментар — будь-який текст є коментарем
    if state == STATE_WAITING_COMMENT:
        _handle_comment_input(update, context)
        return

    # Спроба розпізнати номер телефону
    phone = try_parse_phone(update.message.text)
    if not phone:
        return

    categories = get_all_categories(department)
    if not categories:
        update.message.reply_text("❌ Немає категорій у базі")
        return

    # Зберігаємо стан та показуємо інлайн-кнопки категорій
    context.user_data.clear()
    context.user_data['state'] = STATE_WAITING_CATEGORY
    context.user_data['phone'] = phone
    context.user_data['department'] = department

    update.message.reply_text(
        f"📞 Телефон: {phone}\nОберіть категорію:",
        reply_markup=build_categories_keyboard(categories)
    )

def _handle_comment_input(update: Update, context: CallbackContext):
    """Called when user is in STATE_WAITING_COMMENT and sends a text message."""
    comment = update.message.text.strip()
    phone         = context.user_data['phone']
    code          = context.user_data['category_code']
    category_name = context.user_data['category_name']
    department    = context.user_data['department']

    employee = get_employee_by_telegram_id(update.message.from_user.id, department)
    if employee:
        employee_name  = employee['name']
        responsible_id = employee['bitrix_id']
    else:
        employee_name  = update.message.from_user.full_name
        responsible_id = RESPONSIBLE_ID

    is_duplicate = check_duplicate_record(
        update.message.from_user.id, code, phone, department, minutes=5
    )

    if is_duplicate:
        context.user_data['state'] = STATE_AWAITING_DUPLICATE
        context.user_data['pending_record'] = {
            'employee_telegram_id': update.message.from_user.id,
            'code':          code,
            'phone':         phone,
            'comment':       comment,
            'category_name': category_name,
            'employee_name': employee_name,
            'responsible_id': responsible_id,
            'department':    department,
        }
        update.message.reply_text(
            f"⚠️ Ви вже записували категорію {code} для цього клієнта менше 5 хв тому.\n"
            f"Продовжити?",
            reply_markup=build_duplicate_keyboard()
        )
        return

    context.user_data.clear()
    msg = _save_record(
        employee_telegram_id=update.message.from_user.id,
        code=code, phone=phone, comment=comment,
        category_name=category_name, employee_name=employee_name,
        responsible_id=responsible_id, department=department,
    )
    update.message.reply_text(msg)

# ==========================================
# ОБРОБНИК ІНЛАЙН-КНОПОК
# ==========================================

def handle_callback(update: Update, context: CallbackContext):
    query = update.callback_query
    query.answer()

    data  = query.data
    state = context.user_data.get('state')

    # Вибір категорії
    if data.startswith('cat_') and state == STATE_WAITING_CATEGORY:
        code       = data[4:]
        department = context.user_data.get('department')
        category   = get_category_by_code(code, department)

        if not category:
            query.edit_message_text("❌ Категорія не знайдена")
            return

        context.user_data['category_code'] = code
        context.user_data['category_name'] = category['name']
        context.user_data['state']         = STATE_WAITING_COMMENT

        query.edit_message_text(
            f"📞 Телефон: {context.user_data['phone']}\n"
            f"🧩 Категорія: {category['name']}\n\n"
            f"✏️ Введіть коментар:"
        )

    # Підтвердження дубліката
    elif data in ('dup_yes', 'dup_no') and state == STATE_AWAITING_DUPLICATE:
        if data == 'dup_yes':
            pending = context.user_data.get('pending_record')
            if pending:
                context.user_data.clear()
                msg = _save_record(**pending)
                query.edit_message_text(msg)
            else:
                query.edit_message_text("❌ Помилка: дані не знайдено")
        else:
            query.edit_message_text("❌ Операцію скасовано")
            context.user_data.clear()

# ==========================================
# КОМАНДА: /info
# ==========================================

def handle_info_command(update: Update, context: CallbackContext):
    department = get_department_by_chat_id(update.message.chat_id)
    if not department:
        update.message.reply_text("❌ Ця команда доступна тільки в чатах підтримки або досудебки")
        return

    text = update.message.text.strip()
    m = re.match(r"^/info\s+([+\d()\-\s]+)\s*,\s*(\d+)$", text, re.IGNORECASE)
    if not m:
        update.message.reply_text("Формат: /info +380XXXXXXXXX, N\nНапр.: /info +380631234567, 7")
        return

    phone_raw, days_str = m.groups()
    phone = normalize_phone(phone_raw)
    days  = int(days_str)

    records      = get_records_by_phone(phone, days, department)
    contact      = find_contact_by_phone(phone)
    client_name  = None
    if contact:
        client_name = f"{contact.get('NAME', '')} {contact.get('LAST_NAME', '')}".strip() or None

    total    = len(records)
    since_dt = datetime.now() - timedelta(days=days)
    by_emp   = Counter(r['employee_name'] for r in records if r['employee_name'])
    by_cat   = Counter((r['category_code'], r['category_name']) for r in records if r['category_code'])
    latest   = records[:5]

    header = (
        f"ℹ️ Інформація по клієнту: {client_name or 'Не знайдений у CRM'}\n"
        f"📞 Телефон: {phone}\n"
        f"Період: останні {days} дн. (з {since_dt.strftime('%Y-%m-%d')})"
    )

    emp_block = "👤 За співробітниками:\n" + "\n".join(
        f"   — {emp}: {cnt}" for emp, cnt in by_emp.most_common()
    ) if by_emp else "👤 За співробітниками: —"

    if by_cat:
        cat_lines = [f"   — {name} ({code}): {cnt}" for (code, name), cnt in by_cat.most_common()]
        cat_block = "🧩 По категоріях:\n" + "\n".join(cat_lines)
    else:
        cat_block = "🧩 По категоріях: —"

    if latest:
        last_lines = []
        for r in latest:
            ts       = r['timestamp'].strftime("%Y-%m-%d %H:%M")
            category = r['category_name'] or r['category_code']
            employee = r['employee_name'] or "—"
            comment  = (r['comment'] or "")[:120]
            last_lines.append(f"   • {ts} — {category} — {employee} — {comment}")
        latest_block = "🗒 Останні записи:\n" + "\n".join(last_lines)
    else:
        latest_block = "🗒 Останні записи: —"

    update.message.reply_text(
        "\n".join([header, f"• Звернень: {total}", emp_block, cat_block, latest_block])
    )

# ==========================================
# КОМАНДА: /team_stats
# ==========================================

def handle_team_stats_command(update: Update, context: CallbackContext):
    department = get_department_by_chat_id(update.message.chat_id)
    if not department:
        update.message.reply_text("❌ Ця команда доступна тільки в чатах підтримки або досудебки")
        return

    m = re.match(r"^/team_stats\s+(\d+)$", update.message.text.strip(), re.IGNORECASE)
    if not m:
        update.message.reply_text("Формат: /team_stats N\nНапр.: /team_stats 30")
        return

    days     = int(m.group(1))
    stats    = get_team_stats(days, department)
    since_dt = datetime.now() - timedelta(days=days)

    header = (
        f"👥 Командна статистика за {days} дн.\n"
        f"📅 Період: з {since_dt.strftime('%Y-%m-%d')}\n"
        f"• Загалом звернень: {stats['total']}"
    )

    if stats['by_employee']:
        emp_lines = [
            f"{i}. {emp['name'] or '—'}: {emp['count']} звернень"
            for i, emp in enumerate(stats['by_employee'], 1)
        ]
        emp_block = "\n\n🏆 Топ співробітників:\n" + "\n".join(emp_lines)
    else:
        emp_block = "\n\n🏆 Топ співробітників: —"

    if stats['by_category']:
        cat_lines = [
            f"   — {cat['name'] or cat['code']} ({cat['code']}): {cat['count']}"
            for cat in stats['by_category']
        ]
        cat_block = "\n\n🧩 По категоріях:\n" + "\n".join(cat_lines)
    else:
        cat_block = "\n\n🧩 По категоріях: —"

    update.message.reply_text(header + emp_block + cat_block)

# ==========================================
# КОМАНДА: /export
# ==========================================

def handle_export_command(update: Update, context: CallbackContext):
    department = get_department_by_chat_id(update.message.chat_id)
    if not department:
        update.message.reply_text("❌ Ця команда доступна тільки в чатах підтримки або досудебки")
        return

    m = re.match(r"^/export\s+(\d+)$", update.message.text.strip(), re.IGNORECASE)
    if not m:
        update.message.reply_text("Формат: /export N\nНапр.: /export 30")
        return

    days    = int(m.group(1))
    records = get_all_records(days, department)

    if not records:
        update.message.reply_text("❌ Немає записів за цей період")
        return

    wb = Workbook()
    ws = wb.active
    ws.title = "Звернення"
    ws.append(["Дата/час", "Співробітник", "Категорія", "Телефон клієнта", "Коментар"])

    for r in records:
        ws.append([
            r['timestamp'].strftime("%Y-%m-%d %H:%M:%S"),
            r['employee_name'] or "—",
            f"{r['category_name']} ({r['category_code']})" if r['category_name'] else r['category_code'],
            r['phone'],
            r['comment'] or ""
        ])

    for col in ws.columns:
        max_length = max((len(str(cell.value)) for cell in col if cell.value), default=0)
        ws.column_dimensions[col[0].column_letter].width = min(max_length + 2, 50)

    buffer = BytesIO()
    wb.save(buffer)
    buffer.seek(0)

    update.message.reply_document(
        document=buffer,
        filename=f"export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
        caption=f"📊 Експорт за останні {days} дн. ({len(records)} записів)"
    )

# ==========================================
# КОМАНДА: /list_employees
# ==========================================

def handle_list_employees_command(update: Update, context: CallbackContext):
    department = get_department_by_chat_id(update.message.chat_id)
    if not department:
        update.message.reply_text("❌ Ця команда доступна тільки в чатах підтримки або досудебки")
        return

    employees = get_all_employees(department)
    if not employees:
        update.message.reply_text("❌ Немає співробітників у базі")
        return

    lines = ["👥 Список співробітників:\n"]
    for emp in employees:
        lines.append(
            f"• {emp['name']}\n"
            f"  TG ID: {emp['telegram_id']}\n"
            f"  Bitrix ID: {emp['bitrix_id']}"
        )
    update.message.reply_text("\n".join(lines))

# ==========================================
# КОМАНДА: /list_categories
# ==========================================

def handle_list_categories_command(update: Update, context: CallbackContext):
    department = get_department_by_chat_id(update.message.chat_id)
    if not department:
        update.message.reply_text("❌ Ця команда доступна тільки в чатах підтримки або досудебки")
        return

    categories = get_all_categories(department, use_cache=False)
    if not categories:
        update.message.reply_text("❌ Немає категорій у базі")
        return

    lines = ["🧩 Список категорій:\n"] + [f"• {cat['code']} — {cat['name']}" for cat in categories]
    update.message.reply_text("\n".join(lines))

# ==========================================
# КОМАНДА: /add_employee (тільки для адміна)
# ==========================================

def start_add_employee(update: Update, context: CallbackContext):
    if not is_admin(update.message.from_user.id):
        update.message.reply_text("❌ У вас немає доступу до цієї команди")
        return ConversationHandler.END

    department = get_department_by_chat_id(update.message.chat_id)
    if not department:
        update.message.reply_text("❌ Ця команда доступна тільки в чатах підтримки або досудебки")
        return ConversationHandler.END

    context.user_data['department'] = department
    update.message.reply_text("Введіть Telegram ID співробітника:")
    return ADD_EMPLOYEE_TG_ID

def add_employee_tg_id(update: Update, context: CallbackContext):
    try:
        context.user_data['new_employee_tg_id'] = int(update.message.text.strip())
        update.message.reply_text("Введіть Bitrix ID співробітника:")
        return ADD_EMPLOYEE_BITRIX_ID
    except ValueError:
        update.message.reply_text("❌ Невірний формат. Введіть число (Telegram ID):")
        return ADD_EMPLOYEE_TG_ID

def add_employee_bitrix_id(update: Update, context: CallbackContext):
    try:
        context.user_data['new_employee_bitrix_id'] = int(update.message.text.strip())
        update.message.reply_text("Введіть ПІБ співробітника:")
        return ADD_EMPLOYEE_NAME
    except ValueError:
        update.message.reply_text("❌ Невірний формат. Введіть число (Bitrix ID):")
        return ADD_EMPLOYEE_BITRIX_ID

def add_employee_name(update: Update, context: CallbackContext):
    name       = update.message.text.strip()
    tg_id      = context.user_data['new_employee_tg_id']
    bitrix_id  = context.user_data['new_employee_bitrix_id']
    department = context.user_data['department']

    if add_employee(tg_id, name, bitrix_id, department):
        update.message.reply_text(
            f"✅ Співробітник додано:\n"
            f"• Telegram ID: {tg_id}\n"
            f"• Bitrix ID: {bitrix_id}\n"
            f"• ПІБ: {name}"
        )
    else:
        update.message.reply_text("❌ Помилка при додаванні співробітника")

    context.user_data.clear()
    return ConversationHandler.END

def cancel_conversation(update: Update, context: CallbackContext):
    update.message.reply_text("❌ Операцію скасовано")
    context.user_data.clear()
    return ConversationHandler.END

# ==========================================
# КОМАНДА: /delete_employee (тільки для адміна)
# ==========================================

def handle_delete_employee_command(update: Update, context: CallbackContext):
    if not is_admin(update.message.from_user.id):
        update.message.reply_text("❌ У вас немає доступу до цієї команди")
        return

    department = get_department_by_chat_id(update.message.chat_id)
    if not department:
        update.message.reply_text("❌ Ця команда доступна тільки в чатах підтримки або досудебки")
        return

    m = re.match(r"^/delete_employee\s+(\d+)$", update.message.text.strip(), re.IGNORECASE)
    if not m:
        update.message.reply_text("Формат: /delete_employee TELEGRAM_ID\nНапр.: /delete_employee 123456789")
        return

    tg_id = int(m.group(1))
    if delete_employee(tg_id, department):
        update.message.reply_text(f"✅ Співробітник з Telegram ID {tg_id} видалено")
    else:
        update.message.reply_text(f"❌ Співробітник з Telegram ID {tg_id} не знайдений")

# ==========================================
# КОМАНДА: /add_category (тільки для адміна)
# ==========================================

def start_add_category(update: Update, context: CallbackContext):
    if not is_admin(update.message.from_user.id):
        update.message.reply_text("❌ У вас немає доступу до цієї команди")
        return ConversationHandler.END

    department = get_department_by_chat_id(update.message.chat_id)
    if not department:
        update.message.reply_text("❌ Ця команда доступна тільки в чатах підтримки або досудебки")
        return ConversationHandler.END

    context.user_data['department'] = department
    update.message.reply_text("Введіть код категорії (наприклад, CL1):")
    return ADD_CATEGORY_CODE

def add_category_code(update: Update, context: CallbackContext):
    code = update.message.text.strip().upper()
    if not re.match(r"^[A-Z0-9]{2,10}$", code):
        update.message.reply_text("❌ Невірний формат коду. Використовуйте 2-10 літер/цифр:")
        return ADD_CATEGORY_CODE
    context.user_data['new_category_code'] = code
    update.message.reply_text("Введіть назву категорії:")
    return ADD_CATEGORY_NAME

def add_category_name(update: Update, context: CallbackContext):
    name       = update.message.text.strip()
    code       = context.user_data['new_category_code']
    department = context.user_data['department']

    if add_category(code, name, department):
        update.message.reply_text(f"✅ Категорія додано: {code} — {name}")
    else:
        update.message.reply_text("❌ Помилка при додаванні категорії")

    context.user_data.clear()
    return ConversationHandler.END

# ==========================================
# КОМАНДА: /delete_category (тільки для адміна)
# ==========================================

def handle_delete_category_command(update: Update, context: CallbackContext):
    if not is_admin(update.message.from_user.id):
        update.message.reply_text("❌ У вас немає доступу до цієї команди")
        return

    department = get_department_by_chat_id(update.message.chat_id)
    if not department:
        update.message.reply_text("❌ Ця команда доступна тільки в чатах підтримки або досудебки")
        return

    m = re.match(r"^/delete_category\s+([A-Z0-9]+)$", update.message.text.strip(), re.IGNORECASE)
    if not m:
        update.message.reply_text("Формат: /delete_category CODE\nНапр.: /delete_category CL1")
        return

    code = m.group(1).upper()
    if delete_category(code, department):
        update.message.reply_text(f"✅ Категорію {code} видалено")
    else:
        update.message.reply_text(f"❌ Категорію {code} не знайдено")

# ==========================================
# MAIN
# ==========================================

def main():
    init_pool()

    updater = Updater(BOT_TOKEN, use_context=True)
    dp = updater.dispatcher

    dp.add_handler(CommandHandler("info",             handle_info_command))
    dp.add_handler(CommandHandler("team_stats",       handle_team_stats_command))
    dp.add_handler(CommandHandler("export",           handle_export_command))
    dp.add_handler(CommandHandler("list_employees",   handle_list_employees_command))
    dp.add_handler(CommandHandler("list_categories",  handle_list_categories_command))
    dp.add_handler(CommandHandler("delete_employee",  handle_delete_employee_command))
    dp.add_handler(CommandHandler("delete_category",  handle_delete_category_command))

    dp.add_handler(ConversationHandler(
        entry_points=[CommandHandler("add_employee", start_add_employee)],
        states={
            ADD_EMPLOYEE_TG_ID:    [MessageHandler(Filters.text & ~Filters.command, add_employee_tg_id)],
            ADD_EMPLOYEE_BITRIX_ID:[MessageHandler(Filters.text & ~Filters.command, add_employee_bitrix_id)],
            ADD_EMPLOYEE_NAME:     [MessageHandler(Filters.text & ~Filters.command, add_employee_name)],
        },
        fallbacks=[CommandHandler("cancel", cancel_conversation)]
    ))

    dp.add_handler(ConversationHandler(
        entry_points=[CommandHandler("add_category", start_add_category)],
        states={
            ADD_CATEGORY_CODE: [MessageHandler(Filters.text & ~Filters.command, add_category_code)],
            ADD_CATEGORY_NAME: [MessageHandler(Filters.text & ~Filters.command, add_category_name)],
        },
        fallbacks=[CommandHandler("cancel", cancel_conversation)]
    ))

    dp.add_handler(CallbackQueryHandler(handle_callback))
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_message))

    updater.start_polling()
    print("✅ Бот запущено!")
    updater.idle()

if __name__ == "__main__":
    main()
