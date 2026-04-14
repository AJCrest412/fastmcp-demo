from __future__ import annotations

import calendar
import csv
import io
import json
import os
import tempfile
from datetime import datetime
from typing import Any

import aiosqlite
from fastmcp import FastMCP

# Use a temporary directory (common in remote/sandboxed deployments)
TEMP_DIR = tempfile.gettempdir()
DB_PATH = os.path.join(TEMP_DIR, "expenses.db")
CATEGORIES_PATH = os.path.join(os.path.dirname(__file__), "categories.json")

print(f"Database path: {DB_PATH}")

mcp = FastMCP("ExpenseTracker")

DATE_FMT = "%Y-%m-%d"
DEFAULT_SEARCH_LIMIT = 50
MAX_SEARCH_LIMIT = 200


def _load_categories() -> dict[str, list[str]]:
    with open(CATEGORIES_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    return {str(k).lower(): [str(x).lower() for x in v] for k, v in data.items()}


def _validate_iso_date(value: str, field: str = "date") -> str | dict[str, Any]:
    try:
        datetime.strptime(str(value).strip(), DATE_FMT)
    except (TypeError, ValueError):
        return {"status": "error", "error": f"{field} must be YYYY-MM-DD (e.g. 2026-04-13)."}
    return str(value).strip()


def _validate_date_range(start_date: str, end_date: str) -> dict[str, Any] | None:
    s = _validate_iso_date(start_date, "start_date")
    if isinstance(s, dict):
        return s
    e = _validate_iso_date(end_date, "end_date")
    if isinstance(e, dict):
        return e
    if s > e:
        return {"status": "error", "error": "start_date must be on or before end_date."}
    return None


def _validate_amount(amount: float | int, field: str = "amount") -> float | dict[str, Any]:
    try:
        a = float(amount)
    except (TypeError, ValueError):
        return {"status": "error", "error": f"{field} must be a number."}
    if a <= 0:
        return {"status": "error", "error": f"{field} must be greater than zero."}
    return round(a, 2)


def _validate_category_pair(category: str, subcategory: str) -> tuple[str, str] | dict[str, Any]:
    cats = _load_categories()
    cat = (category or "").strip().lower()
    if not cat:
        return {"status": "error", "error": "category is required."}
    if cat not in cats:
        return {
            "status": "error",
            "error": f"Unknown category '{category}'. Call list_categories() or read expense://categories.",
        }
    sub = (subcategory or "").strip().lower()
    if sub and sub not in cats[cat]:
        return {
            "status": "error",
            "error": f"Unknown subcategory '{subcategory}' for '{cat}'. Allowed: {cats[cat]}",
        }
    return cat, sub


def _month_bounds(year_month: str) -> tuple[str, str] | dict[str, Any]:
    ym = (year_month or "").strip()
    try:
        y, m = map(int, ym.split("-", 1))
        datetime(y, m, 1)
    except (ValueError, TypeError):
        return {"status": "error", "error": "year_month must be YYYY-MM (e.g. 2026-04)."}
    last = calendar.monthrange(y, m)[1]
    start = f"{y:04d}-{m:02d}-01"
    end = f"{y:04d}-{m:02d}-{last:02d}"
    return start, end


def init_db():
    try:
        import sqlite3

        with sqlite3.connect(DB_PATH) as c:
            c.execute("PRAGMA journal_mode=WAL")
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS expenses(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT NOT NULL,
                    amount REAL NOT NULL,
                    category TEXT NOT NULL,
                    subcategory TEXT DEFAULT '',
                    note TEXT DEFAULT ''
                )
                """
            )
        print("Database initialized successfully")
    except Exception as e:
        print(f"Database initialization error: {e}")
        raise


init_db()


@mcp.tool()
async def list_categories():
    """Return all valid category → subcategory lists from categories.json."""
    try:
        return _load_categories()
    except Exception as e:
        return {"status": "error", "error": f"Could not load categories: {str(e)}"}


@mcp.tool()
async def add_expense(date: str, amount: float, category: str, subcategory: str = "", note: str = ""):
    """Add an expense. Dates use YYYY-MM-DD. Category/subcategory must match categories.json."""
    d = _validate_iso_date(date, "date")
    if isinstance(d, dict):
        return d
    a = _validate_amount(amount)
    if isinstance(a, dict):
        return a
    pair = _validate_category_pair(category, subcategory)
    if isinstance(pair, dict):
        return pair
    cat, sub = pair

    try:
        async with aiosqlite.connect(DB_PATH) as c:
            cur = await c.execute(
                "INSERT INTO expenses(date, amount, category, subcategory, note) VALUES (?,?,?,?,?)",
                (d, a, cat, sub, (note or "").strip()),
            )
            await c.commit()
            return {"status": "ok", "id": cur.lastrowid}
    except Exception as e:
        if "readonly" in str(e).lower():
            return {"status": "error", "error": "Database is in read-only mode. Check file permissions."}
        return {"status": "error", "error": f"Database error: {str(e)}"}


@mcp.tool()
async def update_expense(
    expense_id: int,
    date: str | None = None,
    amount: float | None = None,
    category: str | None = None,
    subcategory: str | None = None,
    note: str | None = None,
):
    """Update fields on an existing expense. Only pass fields you want to change."""
    fields: list[str] = []
    values: list[Any] = []

    if date is not None:
        d = _validate_iso_date(date, "date")
        if isinstance(d, dict):
            return d
        fields.append("date = ?")
        values.append(d)

    if amount is not None:
        a = _validate_amount(amount)
        if isinstance(a, dict):
            return a
        fields.append("amount = ?")
        values.append(a)

    if category is not None or subcategory is not None:
        async with aiosqlite.connect(DB_PATH) as c:
            row = await (await c.execute("SELECT category, subcategory FROM expenses WHERE id = ?", (expense_id,))).fetchone()
        if not row:
            return {"status": "error", "error": f"No expense with id {expense_id}."}
        new_cat = (category if category is not None else row[0]).strip().lower()
        new_sub = (subcategory if subcategory is not None else row[1]).strip().lower()
        pair = _validate_category_pair(new_cat, new_sub)
        if isinstance(pair, dict):
            return pair
        cat, sub = pair
        fields.append("category = ?")
        values.append(cat)
        fields.append("subcategory = ?")
        values.append(sub)

    if note is not None:
        fields.append("note = ?")
        values.append((note or "").strip())

    if not fields:
        return {"status": "error", "error": "Provide at least one field to update."}

    values.append(expense_id)
    sql = "UPDATE expenses SET " + ", ".join(fields) + " WHERE id = ?"

    async with aiosqlite.connect(DB_PATH) as c:
        cur = await c.execute(sql, values)
        await c.commit()
        if cur.rowcount == 0:
            return {"status": "error", "error": f"No expense with id {expense_id}."}

    return {"status": "ok", "id": expense_id}


@mcp.tool()
async def delete_expense(expense_id: int):
    """Delete an expense by id."""
    async with aiosqlite.connect(DB_PATH) as c:
        cur = await c.execute("DELETE FROM expenses WHERE id = ?", (expense_id,))
        await c.commit()
        if cur.rowcount == 0:
            return {"status": "error", "error": f"No expense with id {expense_id}."}
    return {"status": "ok", "deleted_id": expense_id}


@mcp.tool()
async def list_expenses(start_date: str, end_date: str):
    """List expenses between start_date and end_date (inclusive), YYYY-MM-DD."""
    err = _validate_date_range(start_date, end_date)
    if err:
        return err

    try:
        async with aiosqlite.connect(DB_PATH) as c:
            cur = await c.execute(
                """
                SELECT id, date, amount, category, subcategory, note
                FROM expenses
                WHERE date BETWEEN ? AND ?
                ORDER BY date ASC, id ASC
                """,
                (start_date.strip(), end_date.strip()),
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in await cur.fetchall()]
    except Exception as e:
        return {"status": "error", "error": f"Error listing expenses: {str(e)}"}


@mcp.tool()
async def summarize(start_date: str, end_date: str, category: str | None = None):
    """Totals per category between two dates (inclusive). Optional filter by category."""
    err = _validate_date_range(start_date, end_date)
    if err:
        return err

    s, e = start_date.strip(), end_date.strip()
    try:
        async with aiosqlite.connect(DB_PATH) as c:
            query = """
                SELECT category, SUM(amount) AS total_amount, COUNT(*) AS count
                FROM expenses
                WHERE date BETWEEN ? AND ?
            """
            params: list[Any] = [s, e]

            if category:
                cat = (category or "").strip().lower()
                pair = _validate_category_pair(cat, "")
                if isinstance(pair, dict):
                    return pair
                query += " AND category = ?"
                params.append(cat)

            query += " GROUP BY category ORDER BY total_amount DESC"

            cur = await c.execute(query, params)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in await cur.fetchall()]
    except Exception as e:
        return {"status": "error", "error": f"Error summarizing expenses: {str(e)}"}


@mcp.tool()
async def monthly_summary(year_month: str):
    """Total spent in a calendar month. year_month is YYYY-MM (e.g. 2026-04)."""
    bounds = _month_bounds(year_month)
    if isinstance(bounds, dict):
        return bounds
    start, end = bounds
    async with aiosqlite.connect(DB_PATH) as c:
        cur = await c.execute(
            "SELECT COALESCE(SUM(amount), 0) AS total_amount FROM expenses WHERE date BETWEEN ? AND ?",
            (start, end),
        )
        row = await cur.fetchone()
    total = float(row[0]) if row else 0.0
    return {"year_month": year_month.strip(), "start_date": start, "end_date": end, "total_amount": round(total, 2)}


@mcp.tool()
async def top_categories(start_date: str, end_date: str, n: int = 5):
    """Top N categories by total amount in a date range (inclusive YYYY-MM-DD)."""
    err = _validate_date_range(start_date, end_date)
    if err:
        return err
    if n < 1:
        return {"status": "error", "error": "n must be at least 1."}
    n = min(int(n), 100)

    async with aiosqlite.connect(DB_PATH) as c:
        cur = await c.execute(
            """
            SELECT category, SUM(amount) AS total_amount
            FROM expenses
            WHERE date BETWEEN ? AND ?
            GROUP BY category
            ORDER BY total_amount DESC
            LIMIT ?
            """,
            (start_date.strip(), end_date.strip(), n),
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in await cur.fetchall()]


@mcp.tool()
async def search_expenses(
    query: str,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int = DEFAULT_SEARCH_LIMIT,
):
    """Search note/category/subcategory (substring, case-insensitive). Optional date bounds YYYY-MM-DD."""
    q = (query or "").strip()
    if not q:
        return {"status": "error", "error": "query must not be empty."}
    if limit < 1 or limit > MAX_SEARCH_LIMIT:
        return {"status": "error", "error": f"limit must be 1..{MAX_SEARCH_LIMIT}."}

    like = f"%{q.lower()}%"
    params: list[Any] = [like, like, like]
    sql = """
        SELECT id, date, amount, category, subcategory, note
        FROM expenses
        WHERE (LOWER(note) LIKE ? OR LOWER(category) LIKE ? OR LOWER(subcategory) LIKE ?)
    """

    if start_date is not None and end_date is not None:
        err = _validate_date_range(start_date, end_date)
        if err:
            return err
        sql += " AND date BETWEEN ? AND ?"
        params.extend([start_date.strip(), end_date.strip()])
    elif start_date is not None or end_date is not None:
        return {"status": "error", "error": "Provide both start_date and end_date, or neither."}

    sql += " ORDER BY date DESC, id DESC LIMIT ?"
    params.append(limit)

    async with aiosqlite.connect(DB_PATH) as c:
        cur = await c.execute(sql, params)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in await cur.fetchall()]


@mcp.tool()
async def export_expenses_csv(start_date: str, end_date: str):
    """Export expenses in the date range as CSV text (header row included)."""
    err = _validate_date_range(start_date, end_date)
    if err:
        return err

    async with aiosqlite.connect(DB_PATH) as c:
        cur = await c.execute(
            """
            SELECT id, date, amount, category, subcategory, note
            FROM expenses
            WHERE date BETWEEN ? AND ?
            ORDER BY date ASC, id ASC
            """,
            (start_date.strip(), end_date.strip()),
        )
        rows = await cur.fetchall()

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["id", "date", "amount", "category", "subcategory", "note"])
    w.writerows(rows)
    return {"status": "ok", "csv": buf.getvalue(), "row_count": len(rows)}


@mcp.resource("expense:///categories", mime_type="application/json")  # Changed: expense:// → expense:///
def categories():
    try:
        # Provide default categories if file doesn't exist
        default_categories = {
            "categories": [
                "Food & Dining",
                "Transportation",
                "Shopping",
                "Entertainment",
                "Bills & Utilities",
                "Healthcare",
                "Travel",
                "Education",
                "Business",
                "Other"
            ]
        }
        
        try:
            with open(CATEGORIES_PATH, "r", encoding="utf-8") as f:
                return f.read()
        except FileNotFoundError:
            import json
            return json.dumps(default_categories, indent=2)
    except Exception as e:
        return f'{{"error": "Could not load categories: {str(e)}"}}'

# Start the server
if __name__ == "__main__":
    mcp.run(transport="http", host="0.0.0.0", port=8000)
    # mcp.run()