"""Database fixture generators for all three DB-ER tasks.

Each task exposes two functions:
  create_corrupted_db(task_id) -> sqlite3.Connection   (agent's working copy)
  create_golden_db(task_id)    -> sqlite3.Connection   (grader reference)

All connections use :memory: so they are lightweight and perfectly isolated
between episodes.  Row factory is set to sqlite3.Row for dict-like access.
"""

from __future__ import annotations

import json
import sqlite3
from typing import Dict, List, Tuple

#  Public API 

TASK_TICKETS: Dict[int, str] = {
    1: (
        "CRITICAL ALERT [P1] -- 2024-03-15 02:34:11 UTC\n"
        "SQLite error: UNIQUE constraint failed: users.email\n"
        "Impact: All user registration and login endpoints returning HTTP 500.\n"
        "~300 failed logins in the last 15 minutes.\n"
        "Context: On-call DBA suspects a 'bulk import' script was executed twice "
        "without deduplication. The users table may contain duplicate email rows. "
        "Application schema expects email to be unique. Restore integrity immediately."
    ),
    2: (
        "CRITICAL ALERT [P1] -- 2024-04-22 14:17:33 UTC\n"
        "SQLite error: FOREIGN KEY constraint failed (purchases.product_id)\n"
        "Impact: Checkout pipeline fully DOWN -- no orders can be processed.\n"
        "Context: DB admin ran an urgent 'product catalogue cleanup' script 20 minutes ago. "
        "Several products were removed from the products table. "
        "Check admin_audit_logs for details on deleted records. "
        "Reconstruct missing products and restore referential integrity."
    ),
    3: (
        "CRITICAL ALERT [P0] -- 2024-12-01 10:17:44 UTC\n"
        "Migration script TIMED OUT after 300 seconds. Process killed.\n"
        "Impact: employees_new is missing >50% of company staff. Payroll run in 2 hours.\n"
        "Context: Migrating employees_old -> employees_new + compensation tables. "
        "Script reported: 'Timeout on INSERT at employee record near ID=3 -- suspect DB lock or network issue.' "
        "WARNING: This timeout message may be MISLEADING. Investigate employees_old carefully "
        "before concluding. Complete the migration while preserving the exact ID-to-salary mapping."
    ),
}

CORE_TABLES: Dict[int, List[str]] = {
    1: ["users"],
    2: ["vendors", "products", "purchases"],
    3: ["employees_old", "employees_new", "compensation"],
}


def create_corrupted_db(task_id: int) -> sqlite3.Connection:
    """Return a fresh in-memory SQLite connection with the corrupted schema/data."""
    conn = _make_conn()
    {1: _task1_corrupted, 2: _task2_corrupted, 3: _task3_corrupted}[task_id](conn)
    conn.commit()
    return conn


def create_golden_db(task_id: int) -> sqlite3.Connection:
    """Return a read-only in-memory SQLite connection with the golden (correct) state."""
    conn = _make_conn()
    {1: _task1_golden, 2: _task2_golden, 3: _task3_golden}[task_id](conn)
    conn.commit()
    return conn


def get_initial_row_counts(conn: sqlite3.Connection, task_id: int) -> Dict[str, int]:
    """Return the initial row counts for all core tables (for informational tracking)."""
    counts = {}
    for table in CORE_TABLES[task_id]:
        row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        counts[table] = row[0]
    return counts


#  Helpers 

def _make_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=OFF")   # let agent control FK enforcement
    return conn


#  TASK 1 : Phantom Duplicates 
#
# Scenario: a bulk-import script ran twice. The users table now contains 3
# duplicate email addresses (newer rows, higher IDs). The original 8 users are
# intact; rows 9-11 are the unwanted duplicates.
#
# Agent goal: DELETE the 3 newer duplicate rows (keep lowest-ID record per email).

_USERS_DDL = """
CREATE TABLE users (
    id          INTEGER PRIMARY KEY,
    email       TEXT    NOT NULL,
    username    TEXT    NOT NULL,
    created_at  TEXT    NOT NULL,
    status      TEXT    NOT NULL DEFAULT 'active'
);
"""

_USERS_ORIGINAL: List[Tuple] = [
    (1, "alice@example.com",  "alice",   "2024-01-15 09:00:00", "active"),
    (2, "bob@example.com",    "bob",     "2024-01-16 10:00:00", "active"),
    (3, "carol@example.com",  "carol",   "2024-01-17 11:00:00", "active"),
    (4, "dave@example.com",   "dave",    "2024-01-18 12:00:00", "active"),
    (5, "eve@example.com",    "eve",     "2024-01-19 13:00:00", "active"),
    (6, "frank@example.com",  "frank",   "2024-01-20 14:00:00", "active"),
    (7, "grace@example.com",  "grace",   "2024-01-21 15:00:00", "active"),
    (8, "henry@example.com",  "henry",   "2024-01-22 16:00:00", "active"),
]

_USERS_DUPLICATES: List[Tuple] = [
    # Duplicate of alice (ID 1) - newer timestamp, slightly different username
    (9,  "alice@example.com", "alice_import2",  "2024-03-01 09:00:00", "active"),
    # Duplicate of bob (ID 2)
    (10, "bob@example.com",   "bob_backup",     "2024-03-02 10:00:00", "inactive"),
    # Duplicate of carol (ID 3)
    (11, "carol@example.com", "carol_v2",       "2024-03-03 11:00:00", "active"),
]


def _task1_corrupted(conn: sqlite3.Connection) -> None:
    conn.execute(_USERS_DDL)
    conn.executemany(
        "INSERT INTO users VALUES (?,?,?,?,?)",
        _USERS_ORIGINAL + _USERS_DUPLICATES,
    )


def _task1_golden(conn: sqlite3.Connection) -> None:
    conn.execute(_USERS_DDL)
    conn.executemany("INSERT INTO users VALUES (?,?,?,?,?)", _USERS_ORIGINAL)


#  TASK 2 : Cascading Failure 
#
# Scenario: a hasty "catalogue cleanup" deleted 3 products that were still
# referenced by existing purchases (FK violations). The audit log recorded the
# deleted rows as JSON - the agent must reconstruct them.
#
# Tables: vendors (5), products (12->9 corrupted), purchases (20), admin_audit_logs

_VENDORS_DDL = """
CREATE TABLE vendors (
    id            INTEGER PRIMARY KEY,
    name          TEXT    NOT NULL,
    contact_email TEXT    NOT NULL,
    country       TEXT    NOT NULL DEFAULT 'US',
    status        TEXT    NOT NULL DEFAULT 'active'
);
"""

_PRODUCTS_DDL = """
CREATE TABLE products (
    id             INTEGER PRIMARY KEY,
    vendor_id      INTEGER NOT NULL,
    name           TEXT    NOT NULL,
    price          REAL    NOT NULL,
    stock_quantity INTEGER NOT NULL DEFAULT 0,
    category       TEXT    NOT NULL DEFAULT 'general',
    FOREIGN KEY (vendor_id) REFERENCES vendors(id)
);
"""

_PURCHASES_DDL = """
CREATE TABLE purchases (
    id            INTEGER PRIMARY KEY,
    product_id    INTEGER NOT NULL,
    buyer_email   TEXT    NOT NULL,
    quantity      INTEGER NOT NULL,
    total_price   REAL    NOT NULL,
    purchase_date TEXT    NOT NULL,
    FOREIGN KEY (product_id) REFERENCES products(id)
);
"""

_AUDIT_DDL = """
CREATE TABLE admin_audit_logs (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    action       TEXT    NOT NULL,
    table_name   TEXT    NOT NULL,
    record_id    INTEGER,
    record_json  TEXT,
    performed_by TEXT    NOT NULL DEFAULT 'system',
    timestamp    TEXT    NOT NULL
);
"""

_VENDORS_DATA: List[Tuple] = [
    (1, "Acme Corp",       "acme@acme.com",       "US",  "active"),
    (2, "ByteForge Ltd",   "info@byteforge.io",   "UK",  "active"),
    (3, "DataSphere Inc",  "ds@datasphere.com",   "US",  "active"),
    (4, "Nexus Supplies",  "nexus@nexussup.com",  "CA",  "active"),
    (5, "TechPulse GmbH",  "tp@techpulse.de",     "DE",  "active"),
]

# All 12 products (golden state)
_PRODUCTS_ALL: List[Tuple] = [
    (1,  1, "Widget Standard",    9.99,  500, "hardware"),
    (2,  1, "Widget Pro",        29.99,  200, "hardware"),
    (3,  2, "DataCable USB-C",    7.49, 1000, "cables"),
    (4,  2, "DataCable Thunderbolt", 24.99, 150, "cables"),   # DELETED in corrupted
    (5,  3, "CloudSync License",  49.99,  999, "software"),
    (6,  3, "CloudSync Enterprise", 199.99, 50, "software"),
    (7,  4, "Office Pack Std",    15.00,  300, "supplies"),
    (8,  4, "Office Pack Premium", 35.00, 180, "supplies"),   # DELETED in corrupted
    (9,  5, "TechPulse Dev Kit",  89.99,   75, "hardware"),
    (10, 5, "TechPulse IoT Sensor", 12.99, 400, "hardware"),
    (11, 2, "DataCable Micro-USB",  4.99, 800, "cables"),     # DELETED in corrupted
    (12, 3, "CloudSync Mobile",   19.99,  250, "software"),
]

# Deleted products: IDs 4, 8, 11
_DELETED_PRODUCT_IDS = {4, 8, 11}
_PRODUCTS_CORRUPTED = [p for p in _PRODUCTS_ALL if p[0] not in _DELETED_PRODUCT_IDS]

# 20 purchases - 6 reference deleted products (IDs 4, 8, 11 -> 2 purchases each)
_PURCHASES_DATA: List[Tuple] = [
    (1,  1, "user001@test.com",  2,  19.98, "2024-04-01"),
    (2,  2, "user002@test.com",  1,  29.99, "2024-04-01"),
    (3,  3, "user003@test.com",  3,  22.47, "2024-04-02"),
    (4,  4, "user004@test.com",  1,  24.99, "2024-04-02"),   # refs DELETED product 4
    (5,  5, "user005@test.com",  1,  49.99, "2024-04-03"),
    (6,  6, "user006@test.com",  1, 199.99, "2024-04-03"),
    (7,  7, "user007@test.com",  4,  60.00, "2024-04-04"),
    (8,  8, "user008@test.com",  2,  70.00, "2024-04-04"),   # refs DELETED product 8
    (9,  9, "user009@test.com",  1,  89.99, "2024-04-05"),
    (10, 10, "user010@test.com", 3,  38.97, "2024-04-05"),
    (11, 11, "user011@test.com", 2,   9.98, "2024-04-06"),   # refs DELETED product 11
    (12, 12, "user012@test.com", 1,  19.99, "2024-04-06"),
    (13,  1, "user013@test.com", 5,  49.95, "2024-04-07"),
    (14,  3, "user014@test.com", 2,  14.98, "2024-04-07"),
    (15,  4, "user015@test.com", 3,  74.97, "2024-04-08"),   # refs DELETED product 4
    (16,  5, "user016@test.com", 2,  99.98, "2024-04-08"),
    (17,  8, "user017@test.com", 1,  35.00, "2024-04-09"),   # refs DELETED product 8
    (18, 11, "user018@test.com", 4,  19.96, "2024-04-09"),   # refs DELETED product 11
    (19,  9, "user019@test.com", 2, 179.98, "2024-04-10"),
    (20, 12, "user020@test.com", 1,  19.99, "2024-04-10"),
]

# Audit log entries for the 3 deleted products
_AUDIT_LOGS: List[Tuple] = [
    (
        "DELETE", "products", 4,
        json.dumps({"id": 4, "vendor_id": 2, "name": "DataCable Thunderbolt",
                    "price": 24.99, "stock_quantity": 150, "category": "cables"}),
        "bob-dba", "2024-04-22 14:05:11",
    ),
    (
        "DELETE", "products", 8,
        json.dumps({"id": 8, "vendor_id": 4, "name": "Office Pack Premium",
                    "price": 35.00, "stock_quantity": 180, "category": "supplies"}),
        "bob-dba", "2024-04-22 14:07:44",
    ),
    (
        "DELETE", "products", 11,
        json.dumps({"id": 11, "vendor_id": 2, "name": "DataCable Micro-USB",
                    "price": 4.99, "stock_quantity": 800, "category": "cables"}),
        "bob-dba", "2024-04-22 14:09:02",
    ),
]


def _task2_corrupted(conn: sqlite3.Connection) -> None:
    conn.execute(_VENDORS_DDL)
    conn.execute(_PRODUCTS_DDL)
    conn.execute(_PURCHASES_DDL)
    conn.execute(_AUDIT_DDL)
    conn.executemany("INSERT INTO vendors VALUES (?,?,?,?,?)", _VENDORS_DATA)
    conn.executemany("INSERT INTO products VALUES (?,?,?,?,?,?)", _PRODUCTS_CORRUPTED)
    conn.executemany("INSERT INTO purchases VALUES (?,?,?,?,?,?)", _PURCHASES_DATA)
    conn.executemany(
        "INSERT INTO admin_audit_logs (action, table_name, record_id, record_json, performed_by, timestamp) "
        "VALUES (?,?,?,?,?,?)",
        _AUDIT_LOGS,
    )


def _task2_golden(conn: sqlite3.Connection) -> None:
    conn.execute(_VENDORS_DDL)
    conn.execute(_PRODUCTS_DDL)
    conn.execute(_PURCHASES_DDL)
    conn.execute(_AUDIT_DDL)
    conn.executemany("INSERT INTO vendors VALUES (?,?,?,?,?)", _VENDORS_DATA)
    conn.executemany("INSERT INTO products VALUES (?,?,?,?,?,?)", _PRODUCTS_ALL)
    conn.executemany("INSERT INTO purchases VALUES (?,?,?,?,?,?)", _PURCHASES_DATA)
    conn.executemany(
        "INSERT INTO admin_audit_logs (action, table_name, record_id, record_json, performed_by, timestamp) "
        "VALUES (?,?,?,?,?,?)",
        _AUDIT_LOGS,
    )


#  TASK 3 : Payroll Black Hole 
#
# Scenario: A payroll migration script crashed partway through. The migration
# moves employees_old -> (employees_new + compensation). The "timeout" in the
# migration log is MISLEADING - the real cause is that employees_old has NO
# primary key and contains 3 duplicate employee IDs injected days ago. The
# duplicates are corrupted rows (zero salary, recent hire date) inserted by
# a rogue script. The migration engine crashed with a UNIQUE constraint error
# when it tried to INSERT the same employee ID twice into employees_new.
#
# Agent goal:
#   1. Detect and DELETE the 3 corrupted duplicate rows from employees_old.
#   2. Complete migration: INSERT remaining employees into employees_new.
#   3. INSERT corresponding records into compensation (preserving salary).

_EMP_OLD_DDL = """
CREATE TABLE employees_old (
    id          INTEGER,
    name        TEXT    NOT NULL,
    department  TEXT    NOT NULL,
    role        TEXT    NOT NULL,
    base_salary REAL    NOT NULL,
    hire_date   TEXT    NOT NULL,
    active      INTEGER NOT NULL DEFAULT 1
);
"""
# Note: NO PRIMARY KEY - intentional, mirrors legacy table design.

_EMP_NEW_DDL = """
CREATE TABLE employees_new (
    id          INTEGER PRIMARY KEY,
    name        TEXT    NOT NULL,
    department  TEXT    NOT NULL,
    role        TEXT    NOT NULL,
    hire_date   TEXT    NOT NULL
);
"""

_COMP_DDL = """
CREATE TABLE compensation (
    employee_id    INTEGER PRIMARY KEY,
    base_salary    REAL    NOT NULL,
    bonus_pct      REAL    NOT NULL DEFAULT 0.0,
    effective_date TEXT    NOT NULL,
    FOREIGN KEY (employee_id) REFERENCES employees_new(id)
);
"""

_MIGLOG_DDL = """
CREATE TABLE migration_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    event       TEXT    NOT NULL,
    employee_id INTEGER,
    message     TEXT,
    timestamp   TEXT    NOT NULL
);
"""

# 15 canonical employees (what should exist after migration)
_EMP_CANONICAL: List[Tuple] = [
    # id, name, dept, role, salary, hire_date, active
    (1,  "Alice Nguyen",      "Engineering",  "Senior Engineer",    110000.0, "2020-03-01", 1),
    (2,  "Bob Martinez",      "Marketing",    "Marketing Lead",      88000.0, "2019-07-15", 1),
    (3,  "Carol Thompson",    "Engineering",  "Staff Engineer",     135000.0, "2018-01-10", 1),
    (4,  "Dave Kim",          "Sales",        "Account Executive",   72000.0, "2021-05-20", 1),
    (5,  "Eve Patel",         "Engineering",  "Junior Engineer",     78000.0, "2022-09-01", 1),
    (6,  "Frank Lee",         "HR",           "HR Manager",          92000.0, "2017-11-30", 1),
    (7,  "Grace Wilson",      "Engineering",  "Principal Engineer", 155000.0, "2016-04-15", 1),
    (8,  "Henry Brown",       "Finance",      "CFO",                185000.0, "2015-08-01", 1),
    (9,  "Iris Garcia",       "Sales",        "Sales Director",     130000.0, "2019-02-14", 1),
    (10, "Jack Robinson",     "Engineering",  "DevOps Engineer",     98000.0, "2021-11-01", 1),
    (11, "Karen White",       "Marketing",    "Content Strategist",  65000.0, "2023-01-15", 1),
    (12, "Liam Johnson",      "Engineering",  "ML Engineer",        125000.0, "2020-06-01", 1),
    (13, "Maya Patel",        "Product",      "Product Manager",    115000.0, "2021-03-22", 1),
    (14, "Nathan Scott",      "Engineering",  "Backend Engineer",    95000.0, "2022-02-14", 1),
    (15, "Olivia Chen",       "Design",       "UX Lead",             88000.0, "2020-09-10", 1),
]

# Corrupted duplicate rows injected by rogue script (same IDs as 3, 7, 12)
_EMP_DUPLICATES: List[Tuple] = [
    # Corrupt duplicate of ID 3 (Carol Thompson) - salary=0, recent suspicious date
    (3,  "carol.t",           "Unknown",      "N/A",                  0.0, "2024-11-28", 0),
    # Corrupt duplicate of ID 7 (Grace Wilson)
    (7,  "grace.w_backup",    "Unknown",      "N/A",                  0.0, "2024-11-29", 0),
    # Corrupt duplicate of ID 12 (Liam Johnson)
    (12, "liam.j_dup",        "Unknown",      "N/A",                  0.0, "2024-11-30", 0),
]

# Only employees 1, 2, 4, 5, 6, 8, 9, 10 were migrated before crash
_ALREADY_MIGRATED_IDS = {1, 2, 4, 5, 6, 8, 9, 10}

_EMP_NEW_MIGRATED = [
    (e[0], e[1], e[2], e[3], e[5])
    for e in _EMP_CANONICAL
    if e[0] in _ALREADY_MIGRATED_IDS
]

_COMP_MIGRATED = [
    (e[0], e[4], 0.0, "2024-12-01")   # (employee_id, salary, bonus_pct, date)
    for e in _EMP_CANONICAL
    if e[0] in _ALREADY_MIGRATED_IDS
]

_MIGRATION_LOG_EVENTS = [
    (None,  "MIGRATION_START",   None,  "Payroll migration v2 started. Source: employees_old. Target: employees_new + compensation.", "2024-12-01 10:00:00"),
    (None, "MIGRATED",           1,     "Employee ID=1 migrated successfully.",  "2024-12-01 10:00:14"),
    (None, "MIGRATED",           2,     "Employee ID=2 migrated successfully.",  "2024-12-01 10:00:28"),
    (None, "MIGRATED",           4,     "Employee ID=4 migrated successfully.",  "2024-12-01 10:00:55"),
    (None, "MIGRATED",           5,     "Employee ID=5 migrated successfully.",  "2024-12-01 10:01:09"),
    (None, "MIGRATED",           6,     "Employee ID=6 migrated successfully.",  "2024-12-01 10:01:24"),
    (None, "MIGRATED",           8,     "Employee ID=8 migrated successfully.",  "2024-12-01 10:02:00"),
    (None, "MIGRATED",           9,     "Employee ID=9 migrated successfully.",  "2024-12-01 10:02:14"),
    (None, "MIGRATED",          10,     "Employee ID=10 migrated successfully.", "2024-12-01 10:02:29"),
    (None, "ERROR",              3,     "UNIQUE constraint failed: employees_new.id -- duplicate employee ID=3 detected in source table. Halting batch.", "2024-12-01 10:02:44"),
    (None, "TIMEOUT",           None,   "Migration process exceeded 300s threshold. Process killed by watchdog. Reason: Timeout on INSERT near record ID=3. Possible DB lock or network issue.", "2024-12-01 10:17:44"),
]


def _task3_corrupted(conn: sqlite3.Connection) -> None:
    conn.execute(_EMP_OLD_DDL)
    conn.execute(_EMP_NEW_DDL)
    conn.execute(_COMP_DDL)
    conn.execute(_MIGLOG_DDL)

    # employees_old: canonical rows + 3 corrupted duplicates (in mixed order for realism)
    all_old_rows = list(_EMP_CANONICAL) + _EMP_DUPLICATES
    # Sort by hire_date so duplicates aren't obviously at the end
    all_old_rows.sort(key=lambda r: (r[5], r[0]))
    conn.executemany("INSERT INTO employees_old VALUES (?,?,?,?,?,?,?)", all_old_rows)

    conn.executemany("INSERT INTO employees_new VALUES (?,?,?,?,?)", _EMP_NEW_MIGRATED)
    conn.executemany(
        "INSERT INTO compensation (employee_id, base_salary, bonus_pct, effective_date) VALUES (?,?,?,?)",
        _COMP_MIGRATED,
    )
    conn.executemany(
        "INSERT INTO migration_log (event, employee_id, message, timestamp) VALUES (?,?,?,?)",
        [(e[1], e[2], e[3], e[4]) for e in _MIGRATION_LOG_EVENTS],
    )


def _task3_golden(conn: sqlite3.Connection) -> None:
    conn.execute(_EMP_OLD_DDL)
    conn.execute(_EMP_NEW_DDL)
    conn.execute(_COMP_DDL)
    conn.execute(_MIGLOG_DDL)

    # Clean employees_old (no duplicates, canonical only)
    conn.executemany("INSERT INTO employees_old VALUES (?,?,?,?,?,?,?)", _EMP_CANONICAL)

    # All 15 employees migrated
    emp_new_all = [(e[0], e[1], e[2], e[3], e[5]) for e in _EMP_CANONICAL]
    conn.executemany("INSERT INTO employees_new VALUES (?,?,?,?,?)", emp_new_all)

    comp_all = [(e[0], e[4], 0.0, "2024-12-01") for e in _EMP_CANONICAL]
    conn.executemany(
        "INSERT INTO compensation (employee_id, base_salary, bonus_pct, effective_date) VALUES (?,?,?,?)",
        comp_all,
    )
    conn.executemany(
        "INSERT INTO migration_log (event, employee_id, message, timestamp) VALUES (?,?,?,?)",
        [(e[1], e[2], e[3], e[4]) for e in _MIGRATION_LOG_EVENTS],
    )


# Golden row counts per task - used as the safety floor for catastrophic-loss checks.
# The agent must not let any core table drop below 90% of the GOLDEN count.
# Using the corrupted count would make fixing Tasks 1 & 3 impossible (they require
# deleting bad rows that the corrupted DB added on top of the real data).
GOLDEN_ROW_COUNTS: Dict[int, Dict[str, int]] = {
    1: {"users": len(_USERS_ORIGINAL)},            # 8
    2: {
        "vendors":   len(_VENDORS_DATA),             # 5
        "products":  len(_PRODUCTS_ALL),             # 12
        "purchases": len(_PURCHASES_DATA),           # 20
    },
    3: {
        "employees_old": len(_EMP_CANONICAL),        # 15
        "employees_new": len(_EMP_CANONICAL),        # 15
        "compensation":  len(_EMP_CANONICAL),        # 15
    },
}
