import csv
import io
import os
import re
import sqlite3
from datetime import datetime
from functools import wraps

from flask import Flask, flash, g, redirect, render_template, request, send_file, session, url_for
from werkzeug.datastructures import FileStorage
from werkzeug.security import check_password_hash, generate_password_hash

try:
    from pypdf import PdfReader
except ImportError:  # pragma: no cover - fallback for older package name
    try:
        from PyPDF2 import PdfReader
    except ImportError:  # pragma: no cover - handled at runtime
        PdfReader = None


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATABASE_PATH = os.path.join(BASE_DIR, "expense.db")
STYLESHEET_PATH = os.path.join(BASE_DIR, "Static", "styles.css")

app = Flask(__name__, template_folder="Templates", static_folder="Static")
app.secret_key = os.getenv("SECRET_KEY", "expense-tracker-dev-secret")
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
)

DATE_FORMATS = (
    "%Y-%m-%d",
    "%d-%m-%Y",
    "%d/%m/%Y",
    "%m/%d/%Y",
    "%Y/%m/%d",
    "%d %b %Y",
    "%d %B %Y",
    "%d %b, %Y",
    "%d %B, %Y",
    "%d %b %Y %H:%M",
    "%d %B %Y %H:%M",
    "%d %b, %Y %I:%M %p",
    "%d %B, %Y %I:%M %p",
    "%Y-%m-%d %H:%M:%S",
)

CATEGORY_KEYWORDS = {
    "Food": ("food", "restaurant", "cafe", "zomato", "swiggy", "dining"),
    "Travel": ("uber", "ola", "rapido", "metro", "bus", "train", "flight", "travel"),
    "Bills": ("electricity", "water", "bill", "utility", "broadband", "internet"),
    "Recharge": ("recharge", "mobile", "prepaid", "postpaid", "airtel", "jio", "vi"),
    "Shopping": ("amazon", "flipkart", "myntra", "store", "shopping"),
    "Health": ("hospital", "clinic", "pharmacy", "medical", "health"),
    "Salary": ("salary", "payroll", "stipend"),
    "Rent": ("rent", "landlord"),
    "Entertainment": ("movie", "netflix", "spotify", "game", "bookmyshow"),
}

SOURCE_LABELS = {
    "manual": "Manual",
    "gpay_import": "Google Pay",
    "bank_import": "Bank",
}


def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(DATABASE_PATH)
        g.db.row_factory = sqlite3.Row
    return g.db


@app.teardown_appcontext
def close_db(_exception):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def init_db():
    db = sqlite3.connect(DATABASE_PATH)
    cursor = db.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            password TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            amount REAL NOT NULL,
            type TEXT NOT NULL CHECK(type IN ('income', 'expense')),
            category TEXT NOT NULL,
            date TEXT NOT NULL,
            description TEXT DEFAULT '',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
        """
    )
    columns = {row[1] for row in cursor.execute("PRAGMA table_info(transactions)").fetchall()}
    if "source" not in columns:
        cursor.execute("ALTER TABLE transactions ADD COLUMN source TEXT NOT NULL DEFAULT 'manual'")
    if "external_id" not in columns:
        cursor.execute("ALTER TABLE transactions ADD COLUMN external_id TEXT")
    cursor.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_transactions_user_external_id
        ON transactions(user_id, external_id)
        WHERE external_id IS NOT NULL
        """
    )
    db.commit()
    db.close()


def normalize_header(value):
    return re.sub(r"[^a-z0-9]+", "", (value or "").strip().lower())


def read_csv_rows(file_storage):
    file_storage.stream.seek(0)
    decoded = io.StringIO(file_storage.stream.read().decode("utf-8-sig"))
    reader = csv.DictReader(decoded)
    if not reader.fieldnames:
        return []
    return list(reader)


def read_pdf_text(file_storage):
    if PdfReader is None:
        raise RuntimeError("PDF support is not installed.")

    file_storage.stream.seek(0)
    reader = PdfReader(file_storage.stream)
    pages = []
    for page in reader.pages:
        pages.append(page.extract_text() or "")
    return "\n".join(pages)


def clean_amount(value):
    if value is None:
        raise ValueError
    cleaned = str(value).strip()
    if not cleaned:
        raise ValueError
    negative = "-" in cleaned or cleaned.startswith("(")
    cleaned = re.sub(r"[^0-9.]", "", cleaned)
    if not cleaned:
        raise ValueError
    amount = float(cleaned)
    return -amount if negative else amount


def parse_date_value(value):
    raw_value = (value or "").strip()
    if not raw_value:
        raise ValueError

    compact_value = re.sub(r"\s+", " ", raw_value)
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(compact_value, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue

    match = re.search(r"\d{1,2}[-/]\d{1,2}[-/]\d{2,4}", compact_value)
    if match:
        return parse_date_value(match.group(0))

    raise ValueError


def infer_transaction_type(raw_type="", amount=None, text=""):
    combined = " ".join(part for part in (str(raw_type), str(text)) if part).lower()

    if amount is not None and amount < 0:
        return "expense"

    expense_terms = ("expense", "debit", "paid", "sent", "purchase", "payment")
    income_terms = ("income", "credit", "received", "refund", "cashback", "salary")

    if any(term in combined for term in income_terms):
        return "income"
    if any(term in combined for term in expense_terms):
        return "expense"
    return "expense"


def infer_category(text, transaction_type):
    lowered = (text or "").lower()
    for category, keywords in CATEGORY_KEYWORDS.items():
        for keyword in keywords:
            if len(keyword) <= 3:
                if re.search(rf"\b{re.escape(keyword)}\b", lowered):
                    return category
            elif keyword in lowered:
                return category
    return "Income" if transaction_type == "income" else "Transfers"


def build_description(*parts):
    cleaned_parts = []
    seen = set()
    for part in parts:
        value = (part or "").strip()
        key = value.lower()
        if value and key not in seen:
            cleaned_parts.append(value)
            seen.add(key)
    return " | ".join(cleaned_parts)


def extract_external_id(*values):
    combined = " ".join(str(value) for value in values if value)
    match = re.search(
        r"(?:utr|upi\s*ref(?:erence)?(?:\s*no)?|(?:upi\s*)?transaction\s*id)\s*[:#-]?\s*([A-Z0-9-]{8,})",
        combined,
        re.IGNORECASE,
    )
    if match:
        return match.group(1).upper()
    for standalone in re.findall(r"\b[A-Z0-9]{10,}\b", combined, re.IGNORECASE):
        token = standalone.upper()
        if token not in {"TRANSACTION", "STATEMENT", "RECEIVED"}:
            return token
    return None


def extract_counterparty(text):
    match = re.search(r"(?:paid to|sent to|to|received from|from)\s+([A-Za-z0-9 .@&_-]{2,60})", text, re.IGNORECASE)
    if match:
        counterparty = re.split(r"\b(?:on|via|using|with|dated)\b", match.group(1), maxsplit=1, flags=re.IGNORECASE)[0]
        return counterparty.strip(" .,-")
    return ""


def parse_gpay_message(text):
    amount_match = re.search(r"(?:rs\.?|inr|₹)\s*([0-9,]+(?:\.\d{1,2})?)", text, re.IGNORECASE)
    date_match = re.search(
        r"(\d{1,2}[-/]\d{1,2}[-/]\d{2,4}|\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}(?:\s+\d{1,2}:\d{2})?)",
        text,
    )
    if not amount_match or not date_match:
        return None

    raw_amount = amount_match.group(1)
    transaction_type = infer_transaction_type(text=text)
    amount = abs(clean_amount(raw_amount))
    counterparty = extract_counterparty(text)
    description = text.strip()

    return {
        "date": parse_date_value(date_match.group(1)),
        "amount": amount,
        "type": transaction_type,
        "category": infer_category(f"{counterparty} {description}", transaction_type),
        "description": counterparty or description[:140],
        "source": "gpay_import",
        "external_id": extract_external_id(text),
    }


def import_transactions(user_id, transactions):
    db = get_db()
    imported_count = 0
    skipped_count = 0

    for txn in transactions:
        if txn.get("external_id"):
            duplicate = db.execute(
                """
                SELECT 1 FROM transactions
                WHERE user_id = ? AND external_id = ?
                """,
                (user_id, txn["external_id"]),
            ).fetchone()
        else:
            duplicate = db.execute(
                """
                SELECT 1 FROM transactions
                WHERE user_id = ? AND date = ? AND amount = ? AND type = ? AND description = ? AND source = ?
                """,
                (
                    user_id,
                    txn["date"],
                    txn["amount"],
                    txn["type"],
                    txn["description"],
                    txn["source"],
                ),
            ).fetchone()

        if duplicate:
            skipped_count += 1
            continue

        db.execute(
            """
            INSERT INTO transactions (user_id, amount, type, category, date, description, source, external_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user_id,
                txn["amount"],
                txn["type"],
                txn["category"],
                txn["date"],
                txn["description"],
                txn["source"],
                txn["external_id"],
            ),
        )
        imported_count += 1

    db.commit()
    return imported_count, skipped_count


def parse_gpay_csv(file_storage):
    raw_rows = read_csv_rows(file_storage)
    if not raw_rows:
        return []

    rows = []

    for row in raw_rows:
        values = {normalize_header(key): (value or "").strip() for key, value in row.items()}
        raw_amount = (
            values.get("amount")
            or values.get("paidamount")
            or values.get("transactionamount")
            or values.get("value")
        )
        raw_date = (
            values.get("date")
            or values.get("transactiondate")
            or values.get("createdat")
            or values.get("timestamp")
        )
        raw_type = values.get("type") or values.get("transactiontype") or values.get("status")
        notes = " ".join(
            part
            for part in (
                values.get("description"),
                values.get("details"),
                values.get("notes"),
                values.get("remark"),
                values.get("paidto"),
                values.get("receivedfrom"),
            )
            if part
        )

        if not raw_amount or not raw_date:
            continue

        amount = clean_amount(raw_amount)
        transaction_type = infer_transaction_type(raw_type=raw_type, amount=amount, text=notes)
        rows.append(
            {
                "date": parse_date_value(raw_date),
                "amount": abs(amount),
                "type": transaction_type,
                "category": infer_category(notes, transaction_type),
                "description": notes or "Imported from Google Pay",
                "source": "gpay_import",
                "external_id": extract_external_id(
                    values.get("utr"),
                    values.get("upirefno"),
                    values.get("transactionid"),
                    notes,
                ),
            }
        )

    return rows


def parse_bank_csv(file_storage):
    raw_rows = read_csv_rows(file_storage)
    if not raw_rows:
        return []

    rows = []
    for row in raw_rows:
        values = {normalize_header(key): (value or "").strip() for key, value in row.items()}
        raw_date = (
            values.get("date")
            or values.get("transactiondate")
            or values.get("valuedate")
            or values.get("postingdate")
        )
        if not raw_date:
            continue

        description = build_description(
            values.get("description"),
            values.get("narration"),
            values.get("remarks"),
            values.get("details"),
            values.get("payee"),
            values.get("merchant"),
        ) or "Imported from bank statement"

        debit_value = values.get("debit") or values.get("withdrawal") or values.get("paidout")
        credit_value = values.get("credit") or values.get("deposit") or values.get("paidin")
        balance_value = values.get("balance") or values.get("closingbalance")
        type_hint = values.get("type") or values.get("transactiontype") or values.get("drcr")

        amount = None
        transaction_type = None

        if debit_value:
            amount = abs(clean_amount(debit_value))
            transaction_type = "expense"
        elif credit_value:
            amount = abs(clean_amount(credit_value))
            transaction_type = "income"
        else:
            raw_amount = values.get("amount") or values.get("transactionamount")
            if not raw_amount:
                continue
            signed_amount = clean_amount(raw_amount)
            transaction_type = infer_transaction_type(raw_type=type_hint, amount=signed_amount, text=description)
            amount = abs(signed_amount)

        if amount == 0:
            continue

        rows.append(
            {
                "date": parse_date_value(raw_date),
                "amount": amount,
                "type": transaction_type,
                "category": infer_category(description, transaction_type),
                "description": description,
                "source": "bank_import",
                "external_id": extract_external_id(
                    values.get("transactionid"),
                    values.get("referenceno"),
                    values.get("chequenumber"),
                    values.get("utr"),
                    values.get("rrn"),
                    description,
                    balance_value,
                ),
            }
        )

    return rows


def parse_gpay_text(blob):
    transactions = []
    chunks = [chunk.strip() for chunk in re.split(r"\n\s*\n", blob) if chunk.strip()]

    if len(chunks) == 1:
        lines = [line.strip() for line in blob.splitlines() if line.strip()]
        chunks = lines

    for chunk in chunks:
        parsed = parse_gpay_message(chunk)
        if parsed:
            transactions.append(parsed)

    return transactions


def parse_gpay_pdf(file_storage):
    pdf_text = read_pdf_text(file_storage)
    lines = [re.sub(r"\s+", " ", line).strip() for line in pdf_text.splitlines()]
    lines = [line for line in lines if line]

    skip_patterns = (
        "Transaction statement",
        "Date & time Transaction details Amount",
        "Date & time",
        "Transaction details",
        "Amount",
    )

    cleaned_lines = []
    previous_line = ""
    for line in lines:
        if any(pattern == line for pattern in skip_patterns):
            continue
        if line.startswith("Page ") or line.startswith("Note:") or "statement period" in line.lower():
            continue
        if re.fullmatch(r"\d{10}, .+", line):
            continue
        if line in {"Sent", "Received"}:
            previous_line = line
            continue
        if re.fullmatch(r"(?:Rs\.?|INR|₹)\s*[\d,]+(?:\.\d{1,2})?", line) and previous_line in {"Sent", "Received"}:
            previous_line = line
            continue
        cleaned_lines.append(line)
        previous_line = line

    transactions = []
    index = 0
    date_pattern = re.compile(r"\d{1,2} [A-Za-z]{3,9}, \d{4}")
    time_pattern = re.compile(r"\d{1,2}:\d{2} [AP]M", re.IGNORECASE)
    amount_pattern = re.compile(r"(?:Rs\.?|INR|₹)\s*([\d,]+(?:\.\d{1,2})?)", re.IGNORECASE)

    while index < len(cleaned_lines):
        current_line = cleaned_lines[index]
        if not date_pattern.fullmatch(current_line):
            index += 1
            continue

        if index + 1 >= len(cleaned_lines) or not time_pattern.fullmatch(cleaned_lines[index + 1]):
            index += 1
            continue

        raw_date = current_line
        raw_time = cleaned_lines[index + 1]
        index += 2

        detail_lines = []
        while index < len(cleaned_lines) and not date_pattern.fullmatch(cleaned_lines[index]):
            detail_lines.append(cleaned_lines[index])
            index += 1

        if not detail_lines:
            continue

        amount_line = detail_lines[-1]
        amount_match = amount_pattern.search(amount_line)
        if not amount_match:
            continue

        amount = abs(clean_amount(amount_match.group(1)))
        details_only = detail_lines[:-1]
        if not details_only:
            continue

        primary_detail = details_only[0]
        transaction_type = infer_transaction_type(text=primary_detail)

        description = re.sub(
            r"^(Paid to|Received from|Collected from|Paid for)\s+",
            "",
            primary_detail,
            flags=re.IGNORECASE,
        ).strip()

        combined_details = " | ".join(details_only)
        transactions.append(
            {
                "date": parse_date_value(f"{raw_date} {raw_time}"),
                "amount": amount,
                "type": transaction_type,
                "category": infer_category(combined_details, transaction_type),
                "description": description or combined_details,
                "source": "gpay_import",
                "external_id": extract_external_id(combined_details),
            }
        )

    return transactions


def parse_bank_pdf(file_storage):
    pdf_text = read_pdf_text(file_storage)
    transactions = []
    lines = [re.sub(r"\s+", " ", line).strip() for line in pdf_text.splitlines()]
    lines = [line for line in lines if line]

    pattern = re.compile(
        r"(?P<date>\d{1,2}[-/]\d{1,2}[-/]\d{2,4})\s+"
        r"(?P<description>.+?)\s+"
        r"(?:(?P<debit>-?(?:Rs\.?|INR|₹)?\s*[\d,]+(?:\.\d{1,2})?)\s+)?"
        r"(?:(?P<credit>(?:Rs\.?|INR|₹)?\s*[\d,]+(?:\.\d{1,2})?)\s+)?"
        r"(?P<balance>(?:Rs\.?|INR|₹)?\s*[\d,]+(?:\.\d{1,2})?)?$",
        re.IGNORECASE,
    )

    for line in lines:
        match = pattern.search(line)
        if not match:
            continue

        raw_date = match.group("date")
        description = match.group("description").strip(" -|")
        debit_value = match.group("debit")
        credit_value = match.group("credit")

        if debit_value:
            amount = abs(clean_amount(debit_value))
            transaction_type = "expense"
        elif credit_value:
            amount = abs(clean_amount(credit_value))
            transaction_type = "income"
        else:
            continue

        transactions.append(
            {
                "date": parse_date_value(raw_date),
                "amount": amount,
                "type": transaction_type,
                "category": infer_category(description, transaction_type),
                "description": description or "Imported from bank statement PDF",
                "source": "bank_import",
                "external_id": extract_external_id(description, line),
            }
        )

    return transactions


def get_uploaded_extension(file_storage):
    if not file_storage or not file_storage.filename:
        return ""
    return os.path.splitext(file_storage.filename)[1].lower()


def clone_upload(file_bytes, filename):
    return FileStorage(stream=io.BytesIO(file_bytes), filename=filename)


def parse_uploaded_transactions(file_storage):
    if not file_storage or not file_storage.filename:
        raise ValueError

    file_bytes = file_storage.read()
    filename = file_storage.filename
    extension = get_uploaded_extension(file_storage)
    candidates = []

    try:
        if extension == ".pdf":
            for parser, source_name in ((parse_gpay_pdf, "Google Pay"), (parse_bank_pdf, "Bank")):
                parsed_rows = parser(clone_upload(file_bytes, filename))
                if parsed_rows:
                    candidates.append((len(parsed_rows), source_name, parsed_rows))
        else:
            for parser, source_name in ((parse_gpay_csv, "Google Pay"), (parse_bank_csv, "Bank")):
                parsed_rows = parser(clone_upload(file_bytes, filename))
                if parsed_rows:
                    candidates.append((len(parsed_rows), source_name, parsed_rows))
    finally:
        file_storage.stream.seek(0)

    if not candidates:
        return None, []

    candidates.sort(key=lambda item: item[0], reverse=True)
    _, detected_source, transactions = candidates[0]
    return detected_source, transactions


def login_required(view):
    @wraps(view)
    def wrapped_view(*args, **kwargs):
        if "user_id" not in session:
            flash("Please log in to continue.", "warning")
            return redirect(url_for("login"))
        return view(*args, **kwargs)

    return wrapped_view


@app.context_processor
def inject_user():
    return {
        "current_user": session.get("username"),
        "current_year": datetime.now().year,
        "static_version": int(os.path.getmtime(STYLESHEET_PATH)) if os.path.exists(STYLESHEET_PATH) else 1,
    }


@app.route("/")
def index():
    if session.get("user_id"):
        return redirect(url_for("dashboard"))
    return render_template("index.html", current_year=datetime.now().year)


@app.route("/register", methods=["GET", "POST"])
def register():
    if session.get("user_id"):
        return redirect(url_for("dashboard"))

    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        confirm_password = request.form.get("confirm_password", "")

        if len(username) < 3:
            flash("Username must be at least 3 characters long.", "error")
            return render_template("register.html", entered_username=username)

        if len(password) < 6:
            flash("Password must be at least 6 characters long.", "error")
            return render_template("register.html", entered_username=username)

        if password != confirm_password:
            flash("Passwords do not match.", "error")
            return render_template("register.html", entered_username=username)

        db = get_db()
        existing_user = db.execute(
            "SELECT id FROM users WHERE username = ?",
            (username,),
        ).fetchone()
        if existing_user:
            flash("That username is already taken.", "error")
            return render_template("register.html", entered_username=username)

        db.execute(
            "INSERT INTO users (username, password) VALUES (?, ?)",
            (username, generate_password_hash(password)),
        )
        db.commit()
        flash("Account created successfully. Please log in.", "success")
        return redirect(url_for("login"))

    return render_template("register.html", entered_username="")


@app.route("/login", methods=["GET", "POST"])
def login():
    if session.get("user_id"):
        return redirect(url_for("dashboard"))

    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")

        db = get_db()
        user = db.execute(
            "SELECT id, username, password FROM users WHERE username = ?",
            (username,),
        ).fetchone()

        if user and check_password_hash(user["password"], password):
            session.clear()
            session["user_id"] = user["id"]
            session["username"] = user["username"]
            flash("Welcome back.", "success")
            return redirect(url_for("dashboard"))

        flash("Invalid username or password.", "error")
        return render_template("login.html", entered_username=username)

    return render_template("login.html", entered_username="")


@app.route("/logout")
def logout():
    session.clear()
    flash("You have been logged out.", "success")
    return redirect(url_for("index"))


@app.route("/dashboard")
@login_required
def dashboard():
    db = get_db()
    show_history = request.args.get("history") == "1"
    page_value = request.args.get("page", "1").strip()
    show_all = request.args.get("show") == "all"

    try:
        current_page = max(1, int(page_value))
    except ValueError:
        current_page = 1

    total_transactions = db.execute(
        """
        SELECT COUNT(*) AS total_count
        FROM transactions
        WHERE user_id = ?
        """,
        (session["user_id"],),
    ).fetchone()["total_count"]

    per_page = 10
    total_pages = max(1, (total_transactions + per_page - 1) // per_page) if total_transactions else 1
    if current_page > total_pages:
        current_page = total_pages

    if show_history and show_all:
        transactions = db.execute(
            """
            SELECT id, amount, type, category, date, description, source
            FROM transactions
            WHERE user_id = ?
            ORDER BY date DESC, id DESC
            """,
            (session["user_id"],),
        ).fetchall()
    elif show_history:
        offset = (current_page - 1) * per_page
        transactions = db.execute(
            """
            SELECT id, amount, type, category, date, description, source
            FROM transactions
            WHERE user_id = ?
            ORDER BY date DESC, id DESC
            LIMIT ? OFFSET ?
            """,
            (session["user_id"], per_page, offset),
        ).fetchall()
    else:
        transactions = []

    recent_expenses = db.execute(
        """
        SELECT id, amount, type, category, date, description, source
        FROM transactions
        WHERE user_id = ? AND type = 'expense'
        ORDER BY date DESC, id DESC
        LIMIT 5
        """,
        (session["user_id"],),
    ).fetchall()

    totals = db.execute(
        """
        SELECT
            COALESCE(SUM(CASE WHEN type = 'income' THEN amount END), 0) AS total_income,
            COALESCE(SUM(CASE WHEN type = 'expense' THEN amount END), 0) AS total_expense
        FROM transactions
        WHERE user_id = ?
        """,
        (session["user_id"],),
    ).fetchone()

    total_income = float(totals["total_income"] or 0)
    total_expense = float(totals["total_expense"] or 0)
    balance = total_income - total_expense
    can_spend = balance if balance > 0 else 0
    spending_rate = (total_expense / total_income * 100) if total_income > 0 else 0

    return render_template(
        "dashboard.html",
        transactions=transactions,
        total_income=total_income,
        total_expense=total_expense,
        balance=balance,
        can_spend=can_spend,
        spending_rate=spending_rate,
        recent_expenses=recent_expenses,
        source_labels=SOURCE_LABELS,
        show_history=show_history,
        current_page=current_page,
        total_pages=total_pages,
        total_transactions=total_transactions,
        show_all=show_all,
        has_previous=show_history and not show_all and current_page > 1,
        has_next=show_history and not show_all and current_page < total_pages,
        today=datetime.now().strftime("%Y-%m-%d"),
    )


@app.route("/add", methods=["POST"])
@login_required
def add_transaction():
    date_value = request.form.get("date", "").strip()
    amount_value = request.form.get("amount", "").strip()
    category = request.form.get("category", "").strip()
    transaction_type = request.form.get("type", "").strip().lower()
    description = request.form.get("description", "").strip()

    try:
        amount = float(amount_value)
        if amount <= 0:
            raise ValueError
    except ValueError:
        flash("Amount must be a number greater than 0.", "error")
        return redirect(url_for("dashboard"))

    try:
        datetime.strptime(date_value, "%Y-%m-%d")
    except ValueError:
        flash("Please choose a valid date.", "error")
        return redirect(url_for("dashboard"))

    if transaction_type not in {"income", "expense"}:
        flash("Please choose income or expense.", "error")
        return redirect(url_for("dashboard"))

    if not category:
        flash("Category is required.", "error")
        return redirect(url_for("dashboard"))

    db = get_db()
    db.execute(
        """
        INSERT INTO transactions (user_id, amount, type, category, date, description, source)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (session["user_id"], amount, transaction_type, category, date_value, description, "manual"),
    )
    db.commit()

    flash("Transaction added successfully.", "success")
    return redirect(url_for("dashboard"))


@app.route("/import-transactions", methods=["POST"])
@login_required
def import_transactions_file():
    uploaded_file = request.files.get("transaction_file")

    if not uploaded_file or not uploaded_file.filename:
        flash("Upload a statement or transaction file to import.", "warning")
        return redirect(url_for("dashboard"))

    try:
        detected_source, transactions = parse_uploaded_transactions(uploaded_file)
    except (RuntimeError, UnicodeDecodeError, csv.Error, ValueError):
        flash("Couldn't read that file. Upload a transaction CSV or PDF export.", "error")
        return redirect(url_for("dashboard"))

    if not transactions:
        flash("No valid transactions were found in that file.", "warning")
        return redirect(url_for("dashboard"))

    imported_count, skipped_count = import_transactions(session["user_id"], transactions)
    flash(
        f"{detected_source} import finished. Added {imported_count} transaction(s) and skipped {skipped_count} duplicate(s).",
        "success" if imported_count else "warning",
    )
    return redirect(url_for("dashboard"))


@app.route("/export-csv")
@login_required
def export_csv():
    db = get_db()
    rows = db.execute(
        """
        SELECT date, type, amount, category, description
        FROM transactions
        WHERE user_id = ?
        ORDER BY date DESC, id DESC
        """,
        (session["user_id"],),
    ).fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Date", "Type", "Amount", "Category", "Description"])
    writer.writerows(
        [
            [row["date"], row["type"], row["amount"], row["category"], row["description"]]
            for row in rows
        ]
    )
    output.seek(0)

    return send_file(
        io.BytesIO(output.getvalue().encode("utf-8")),
        mimetype="text/csv",
        as_attachment=True,
        download_name="transactions.csv",
    )


@app.route("/add-expense")
@login_required
def add_expense_page():
    return redirect(url_for("dashboard"))


init_db()


if __name__ == "__main__":
    app.run(debug=True)
