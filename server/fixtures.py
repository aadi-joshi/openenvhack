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
    4: (
        "CRITICAL ALERT [P1] -- 2025-01-15 08:22:03 UTC\n"
        "Application error: column 'product_name' not found in table 'inventory'\n"
        "Impact: Warehouse management dashboard returning blank pages. "
        "All inventory queries failing with schema mismatch errors.\n"
        "Context: Junior DBA ran a schema update script last night to 'standardize column names'. "
        "The script renamed several columns in the inventory table and changed the "
        "categories table structure. The application code expects the original column names. "
        "A backup of the original schema is in the schema_changelog table. "
        "Restore the original schema while preserving all data."
    ),
    5: (
        "CRITICAL ALERT [P0] -- 2025-02-10 03:45:12 UTC\n"
        "Batch job FAILED: integrity constraint violation across project tables.\n"
        "Impact: Quarterly budget reconciliation cannot run. Finance team blocked.\n"
        "Context: A cross-database sync job attempted to merge records from a staging "
        "environment into production. The job crashed partway through, reporting a "
        "network timeout at the 'budgets' table insert. Some records may be missing "
        "or duplicated. WARNING: Not all missing records need to be restored -- some "
        "projects were deliberately decommissioned last quarter and their absence is "
        "intentional. Check the decommission_log table before re-inserting anything. "
        "The sync_audit table has partial snapshots of what the job was trying to write."
    ),
}

CORE_TABLES: Dict[int, List[str]] = {
    1: ["users"],
    2: ["vendors", "products", "purchases"],
    3: ["employees_old", "employees_new", "compensation"],
    4: ["inventory", "categories"],
    5: ["departments", "projects", "assignments", "budgets"],
}



def create_corrupted_db(task_id: int) -> sqlite3.Connection:
    """Return a fresh in-memory SQLite connection with the corrupted schema/data."""
    conn = _make_conn()
    {1: _task1_corrupted, 2: _task2_corrupted, 3: _task3_corrupted,
     4: _task4_corrupted, 5: _task5_corrupted}[task_id](conn)
    conn.commit()
    return conn


def create_golden_db(task_id: int) -> sqlite3.Connection:
    """Return a read-only in-memory SQLite connection with the golden (correct) state."""
    conn = _make_conn()
    {1: _task1_golden, 2: _task2_golden, 3: _task3_golden,
     4: _task4_golden, 5: _task5_golden}[task_id](conn)
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
    4: {
        "inventory":  10,
        "categories": 5,
    },
    5: {
        "departments": 6,
        "projects":    8,
        "assignments": 11,
        "budgets":     8,
    },
}


# ── TASK 4 : Schema Drift ──────────────────────────────────────────────
#
# Scenario: A junior DBA ran a "standardization" script that renamed columns
# in the inventory table and restructured the categories table. The application
# code expects the original column names. The schema_changelog table contains
# a record of what was changed.
#
# Agent goal:
#   1. Read schema_changelog to understand the original column names
#   2. Recreate the inventory table with original column names, preserving data
#   3. Recreate the categories table with original column names, preserving data
#
# This tests schema-level reasoning, not just data manipulation.

_CATEGORIES_GOLDEN_DDL = """
CREATE TABLE categories (
    id          INTEGER PRIMARY KEY,
    name        TEXT    NOT NULL,
    description TEXT    NOT NULL DEFAULT '',
    parent_id   INTEGER,
    active      INTEGER NOT NULL DEFAULT 1
);
"""

_CATEGORIES_CORRUPTED_DDL = """
CREATE TABLE categories (
    id              INTEGER PRIMARY KEY,
    category_label  TEXT    NOT NULL,
    detail_text     TEXT    NOT NULL DEFAULT '',
    parent_ref      INTEGER,
    is_enabled      INTEGER NOT NULL DEFAULT 1
);
"""

_INVENTORY_GOLDEN_DDL = """
CREATE TABLE inventory (
    id             INTEGER PRIMARY KEY,
    product_name   TEXT    NOT NULL,
    category_id    INTEGER NOT NULL,
    quantity       INTEGER NOT NULL DEFAULT 0,
    unit_price     REAL    NOT NULL,
    warehouse      TEXT    NOT NULL DEFAULT 'main',
    last_updated   TEXT    NOT NULL,
    FOREIGN KEY (category_id) REFERENCES categories(id)
);
"""

_INVENTORY_CORRUPTED_DDL = """
CREATE TABLE inventory (
    id             INTEGER PRIMARY KEY,
    item_label     TEXT    NOT NULL,
    cat_ref        INTEGER NOT NULL,
    qty_on_hand    INTEGER NOT NULL DEFAULT 0,
    price_per_unit REAL    NOT NULL,
    storage_loc    TEXT    NOT NULL DEFAULT 'main',
    modified_at    TEXT    NOT NULL,
    FOREIGN KEY (cat_ref) REFERENCES categories(id)
);
"""

_CHANGELOG_DDL = """
CREATE TABLE schema_changelog (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name      TEXT    NOT NULL,
    change_type     TEXT    NOT NULL,
    old_column      TEXT,
    new_column      TEXT,
    performed_by    TEXT    NOT NULL DEFAULT 'system',
    timestamp       TEXT    NOT NULL
);
"""

_CATEGORIES_DATA: List[Tuple] = [
    (1, "Electronics",    "Consumer electronics and gadgets",  None, 1),
    (2, "Cables",         "Connectivity cables and adapters",  1,    1),
    (3, "Software",       "Software licenses and subscriptions", None, 1),
    (4, "Office",         "Office supplies and equipment",     None, 1),
    (5, "Peripherals",    "Computer peripherals",              1,    1),
]

_INVENTORY_DATA: List[Tuple] = [
    (1,  "USB-C Hub 7-Port",       1,  45, 34.99, "main",    "2025-01-10"),
    (2,  "Cat6 Ethernet 10m",      2, 200,  8.49, "main",    "2025-01-10"),
    (3,  "Cloud IDE License",      3,  50, 29.99, "east",    "2025-01-10"),
    (4,  "Standing Desk Mat",      4,  30, 44.99, "main",    "2025-01-10"),
    (5,  "Mechanical Keyboard",    5,  75, 89.99, "west",    "2025-01-10"),
    (6,  "Thunderbolt Cable 2m",   2, 150, 19.99, "main",    "2025-01-10"),
    (7,  "Antivirus Suite 1yr",    3, 100, 39.99, "east",    "2025-01-10"),
    (8,  "Ergonomic Mouse",        5,  60, 59.99, "west",    "2025-01-10"),
    (9,  "Webcam HD 1080p",        5,  40, 49.99, "main",    "2025-01-10"),
    (10, "Whiteboard Markers 12pk", 4, 300,  7.99, "main",   "2025-01-10"),
]

_SCHEMA_CHANGELOG: List[Tuple] = [
    (None, "inventory", "RENAME_COLUMN", "product_name", "item_label",     "junior_dba", "2025-01-14 23:10:00"),
    (None, "inventory", "RENAME_COLUMN", "category_id",  "cat_ref",        "junior_dba", "2025-01-14 23:10:01"),
    (None, "inventory", "RENAME_COLUMN", "quantity",      "qty_on_hand",   "junior_dba", "2025-01-14 23:10:02"),
    (None, "inventory", "RENAME_COLUMN", "unit_price",    "price_per_unit","junior_dba", "2025-01-14 23:10:03"),
    (None, "inventory", "RENAME_COLUMN", "warehouse",     "storage_loc",   "junior_dba", "2025-01-14 23:10:04"),
    (None, "inventory", "RENAME_COLUMN", "last_updated",  "modified_at",   "junior_dba", "2025-01-14 23:10:05"),
    (None, "categories", "RENAME_COLUMN", "name",         "category_label","junior_dba", "2025-01-14 23:11:00"),
    (None, "categories", "RENAME_COLUMN", "description",  "detail_text",   "junior_dba", "2025-01-14 23:11:01"),
    (None, "categories", "RENAME_COLUMN", "parent_id",    "parent_ref",    "junior_dba", "2025-01-14 23:11:02"),
    (None, "categories", "RENAME_COLUMN", "active",       "is_enabled",    "junior_dba", "2025-01-14 23:11:03"),
]


def _task4_corrupted(conn: sqlite3.Connection) -> None:
    conn.execute(_CATEGORIES_CORRUPTED_DDL)
    conn.execute(_INVENTORY_CORRUPTED_DDL)
    conn.execute(_CHANGELOG_DDL)
    # Insert data using corrupted column names
    conn.executemany(
        "INSERT INTO categories (id, category_label, detail_text, parent_ref, is_enabled) VALUES (?,?,?,?,?)",
        _CATEGORIES_DATA,
    )
    conn.executemany(
        "INSERT INTO inventory (id, item_label, cat_ref, qty_on_hand, price_per_unit, storage_loc, modified_at) "
        "VALUES (?,?,?,?,?,?,?)",
        _INVENTORY_DATA,
    )
    conn.executemany(
        "INSERT INTO schema_changelog (table_name, change_type, old_column, new_column, performed_by, timestamp) "
        "VALUES (?,?,?,?,?,?)",
        [(c[1], c[2], c[3], c[4], c[5], c[6]) for c in _SCHEMA_CHANGELOG],
    )


def _task4_golden(conn: sqlite3.Connection) -> None:
    conn.execute(_CATEGORIES_GOLDEN_DDL)
    conn.execute(_INVENTORY_GOLDEN_DDL)
    conn.execute(_CHANGELOG_DDL)
    conn.executemany(
        "INSERT INTO categories VALUES (?,?,?,?,?)",
        _CATEGORIES_DATA,
    )
    conn.executemany(
        "INSERT INTO inventory VALUES (?,?,?,?,?,?,?)",
        _INVENTORY_DATA,
    )
    conn.executemany(
        "INSERT INTO schema_changelog (table_name, change_type, old_column, new_column, performed_by, timestamp) "
        "VALUES (?,?,?,?,?,?)",
        [(c[1], c[2], c[3], c[4], c[5], c[6]) for c in _SCHEMA_CHANGELOG],
    )


# ── TASK 5 : Referential Maze ──────────────────────────────────────────
#
# Scenario: A cross-database sync job crashed partway through. The production
# database has missing records, orphaned FK references, and partial data
# scattered across audit tables. Some projects were deliberately decommissioned
# and should NOT be restored.
#
# Agent goal:
#   1. Identify which projects are missing from the projects table
#   2. Check decommission_log to avoid restoring decommissioned projects
#   3. Use sync_audit to find the data for legitimately missing records
#   4. Restore missing departments, projects, assignments, and budgets
#      in the correct FK order (departments first, then projects, etc.)
#
# Trap: The incident ticket blames "budgets table" but the real problem
# spans all 4 core tables. Also, 2 of the "missing" projects are
# decommissioned and must NOT be restored.

_DEPT_DDL = """
CREATE TABLE departments (
    id      INTEGER PRIMARY KEY,
    name    TEXT    NOT NULL,
    head    TEXT    NOT NULL,
    floor   INTEGER NOT NULL DEFAULT 1
);
"""

_PROJ_DDL = """
CREATE TABLE projects (
    id            INTEGER PRIMARY KEY,
    department_id INTEGER NOT NULL,
    name          TEXT    NOT NULL,
    status        TEXT    NOT NULL DEFAULT 'active',
    start_date    TEXT    NOT NULL,
    FOREIGN KEY (department_id) REFERENCES departments(id)
);
"""

_ASSIGN_DDL = """
CREATE TABLE assignments (
    id          INTEGER PRIMARY KEY,
    project_id  INTEGER NOT NULL,
    employee    TEXT    NOT NULL,
    role        TEXT    NOT NULL DEFAULT 'contributor',
    hours       INTEGER NOT NULL DEFAULT 40,
    FOREIGN KEY (project_id) REFERENCES projects(id)
);
"""

_BUDGET_DDL = """
CREATE TABLE budgets (
    id          INTEGER PRIMARY KEY,
    project_id  INTEGER NOT NULL,
    quarter     TEXT    NOT NULL,
    amount      REAL    NOT NULL,
    approved    INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (project_id) REFERENCES projects(id)
);
"""

_DECOM_DDL = """
CREATE TABLE decommission_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type     TEXT    NOT NULL,
    entity_id       INTEGER NOT NULL,
    reason          TEXT    NOT NULL,
    decommissioned  TEXT    NOT NULL
);
"""

_SYNC_AUDIT_DDL = """
CREATE TABLE sync_audit (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name  TEXT    NOT NULL,
    record_id   INTEGER NOT NULL,
    record_json TEXT    NOT NULL,
    sync_status TEXT    NOT NULL DEFAULT 'pending',
    timestamp   TEXT    NOT NULL
);
"""

# 6 departments (all present in both corrupted and golden)
_DEPT_DATA: List[Tuple] = [
    (1, "Engineering",   "Alice Chen",    3),
    (2, "Marketing",     "Bob Smith",     2),
    (3, "Finance",       "Carol Davis",   4),
    (4, "Operations",    "Dave Wilson",   1),
    (5, "Research",      "Eve Martinez",  5),
    (6, "Legal",         "Frank Brown",   4),
]

# 10 projects in golden state. In corrupted: projects 3, 5, 7, 9 are missing.
# BUT projects 5 and 9 are decommissioned (should NOT be restored).
# Only projects 3 and 7 should be re-inserted.
_PROJ_ALL: List[Tuple] = [
    (1,  1, "Backend Rewrite",       "active",     "2024-06-01"),
    (2,  2, "Brand Refresh",         "active",     "2024-07-15"),
    (3,  1, "API Gateway",           "active",     "2024-08-01"),  # MISSING, restore
    (4,  3, "Budget Dashboard",      "active",     "2024-09-01"),
    (5,  5, "Legacy Cleanup",        "completed",  "2023-01-10"),  # MISSING, DECOMMISSIONED
    (6,  4, "Warehouse Automation",  "active",     "2024-10-15"),
    (7,  2, "Customer Portal",       "active",     "2024-11-01"),  # MISSING, restore
    (8,  6, "Compliance Audit",      "active",     "2024-11-15"),
    (9,  5, "Old Analytics",         "completed",  "2022-06-01"),  # MISSING, DECOMMISSIONED
    (10, 3, "Tax Automation",        "active",     "2025-01-01"),
]

_MISSING_PROJECT_IDS = {3, 5, 7, 9}
_DECOMMISSIONED_IDS = {5, 9}   # these should NOT be restored
_RESTORE_PROJECT_IDS = {3, 7}  # these SHOULD be restored

_PROJ_CORRUPTED = [p for p in _PROJ_ALL if p[0] not in _MISSING_PROJECT_IDS]
# Golden projects exclude decommissioned ones
_PROJ_GOLDEN = [p for p in _PROJ_ALL if p[0] not in _DECOMMISSIONED_IDS]

# 15 assignments (some reference missing projects)
_ASSIGN_ALL: List[Tuple] = [
    (1,  1, "Alice Chen",      "lead",        40),
    (2,  1, "Grace Lee",       "contributor", 30),
    (3,  2, "Bob Smith",       "lead",        40),
    (4,  3, "Hank Miller",     "lead",        40),  # refs missing project 3
    (5,  3, "Ivy Taylor",      "contributor", 25),  # refs missing project 3
    (6,  4, "Carol Davis",     "lead",        35),
    (7,  6, "Dave Wilson",     "lead",        40),
    (8,  7, "Jack Anderson",   "lead",        40),  # refs missing project 7
    (9,  7, "Karen Thomas",    "contributor", 20),  # refs missing project 7
    (10, 8, "Frank Brown",     "lead",        40),
    (11, 10, "Leo Garcia",     "lead",        40),
    (12, 1, "Mia Johnson",     "contributor", 15),
    (13, 4, "Noah Williams",   "contributor", 30),
    (14, 6, "Olivia Martinez", "contributor", 25),
    (15, 10, "Paul Robinson",  "contributor", 20),
]

_ASSIGN_CORRUPTED = [a for a in _ASSIGN_ALL if a[1] not in _MISSING_PROJECT_IDS]
# Golden includes assignments for restored projects but not decommissioned
_ASSIGN_GOLDEN = [a for a in _ASSIGN_ALL if a[1] not in _DECOMMISSIONED_IDS]

# 10 budgets (some reference missing projects)
_BUDGET_ALL: List[Tuple] = [
    (1,  1, "2025-Q1", 150000.0, 1),
    (2,  2, "2025-Q1",  80000.0, 1),
    (3,  3, "2025-Q1", 120000.0, 1),  # refs missing project 3
    (4,  4, "2025-Q1",  95000.0, 1),
    (5,  6, "2025-Q1", 200000.0, 1),
    (6,  7, "2025-Q1",  60000.0, 0),  # refs missing project 7
    (7,  8, "2025-Q1",  45000.0, 1),
    (8,  10, "2025-Q1", 110000.0, 1),
    (9,  1, "2025-Q2", 160000.0, 0),
    (10, 4, "2025-Q2", 100000.0, 0),
]

_BUDGET_CORRUPTED = [b for b in _BUDGET_ALL if b[1] not in _MISSING_PROJECT_IDS]
_BUDGET_GOLDEN = [b for b in _BUDGET_ALL if b[1] not in _DECOMMISSIONED_IDS]

# Decommission log - records for projects 5 and 9
_DECOM_DATA: List[Tuple] = [
    (None, "project", 5, "Project completed and archived. All deliverables transferred.", "2024-06-15"),
    (None, "project", 9, "Legacy system sunset. Replaced by modern analytics platform.", "2024-03-01"),
]

# Sync audit - contains the data needed to restore projects 3 and 7
# Also contains entries for decommissioned projects (trap: agent must skip these)
_SYNC_AUDIT_DATA: List[Tuple] = [
    (None, "projects",    3, json.dumps({"id": 3, "department_id": 1, "name": "API Gateway", "status": "active", "start_date": "2024-08-01"}), "failed", "2025-02-10 03:40:01"),
    (None, "projects",    5, json.dumps({"id": 5, "department_id": 5, "name": "Legacy Cleanup", "status": "completed", "start_date": "2023-01-10"}), "failed", "2025-02-10 03:40:02"),
    (None, "projects",    7, json.dumps({"id": 7, "department_id": 2, "name": "Customer Portal", "status": "active", "start_date": "2024-11-01"}), "failed", "2025-02-10 03:40:03"),
    (None, "projects",    9, json.dumps({"id": 9, "department_id": 5, "name": "Old Analytics", "status": "completed", "start_date": "2022-06-01"}), "failed", "2025-02-10 03:40:04"),
    (None, "assignments", 4, json.dumps({"id": 4, "project_id": 3, "employee": "Hank Miller", "role": "lead", "hours": 40}), "failed", "2025-02-10 03:41:01"),
    (None, "assignments", 5, json.dumps({"id": 5, "project_id": 3, "employee": "Ivy Taylor", "role": "contributor", "hours": 25}), "failed", "2025-02-10 03:41:02"),
    (None, "assignments", 8, json.dumps({"id": 8, "project_id": 7, "employee": "Jack Anderson", "role": "lead", "hours": 40}), "failed", "2025-02-10 03:41:03"),
    (None, "assignments", 9, json.dumps({"id": 9, "project_id": 7, "employee": "Karen Thomas", "role": "contributor", "hours": 20}), "failed", "2025-02-10 03:41:04"),
    (None, "budgets",     3, json.dumps({"id": 3, "project_id": 3, "quarter": "2025-Q1", "amount": 120000.0, "approved": 1}), "failed", "2025-02-10 03:42:01"),
    (None, "budgets",     6, json.dumps({"id": 6, "project_id": 7, "quarter": "2025-Q1", "amount": 60000.0, "approved": 0}), "failed", "2025-02-10 03:42:02"),
]


def _task5_corrupted(conn: sqlite3.Connection) -> None:
    conn.execute(_DEPT_DDL)
    conn.execute(_PROJ_DDL)
    conn.execute(_ASSIGN_DDL)
    conn.execute(_BUDGET_DDL)
    conn.execute(_DECOM_DDL)
    conn.execute(_SYNC_AUDIT_DDL)

    conn.executemany("INSERT INTO departments VALUES (?,?,?,?)", _DEPT_DATA)
    conn.executemany("INSERT INTO projects VALUES (?,?,?,?,?)", _PROJ_CORRUPTED)
    conn.executemany("INSERT INTO assignments VALUES (?,?,?,?,?)", _ASSIGN_CORRUPTED)
    conn.executemany("INSERT INTO budgets VALUES (?,?,?,?,?)", _BUDGET_CORRUPTED)
    conn.executemany(
        "INSERT INTO decommission_log (entity_type, entity_id, reason, decommissioned) VALUES (?,?,?,?)",
        [(d[1], d[2], d[3], d[4]) for d in _DECOM_DATA],
    )
    conn.executemany(
        "INSERT INTO sync_audit (table_name, record_id, record_json, sync_status, timestamp) VALUES (?,?,?,?,?)",
        [(s[1], s[2], s[3], s[4], s[5]) for s in _SYNC_AUDIT_DATA],
    )


def _task5_golden(conn: sqlite3.Connection) -> None:
    conn.execute(_DEPT_DDL)
    conn.execute(_PROJ_DDL)
    conn.execute(_ASSIGN_DDL)
    conn.execute(_BUDGET_DDL)
    conn.execute(_DECOM_DDL)
    conn.execute(_SYNC_AUDIT_DDL)

    conn.executemany("INSERT INTO departments VALUES (?,?,?,?)", _DEPT_DATA)
    conn.executemany("INSERT INTO projects VALUES (?,?,?,?,?)", _PROJ_GOLDEN)
    conn.executemany("INSERT INTO assignments VALUES (?,?,?,?,?)", _ASSIGN_GOLDEN)
    conn.executemany("INSERT INTO budgets VALUES (?,?,?,?,?)", _BUDGET_GOLDEN)
    conn.executemany(
        "INSERT INTO decommission_log (entity_type, entity_id, reason, decommissioned) VALUES (?,?,?,?)",
        [(d[1], d[2], d[3], d[4]) for d in _DECOM_DATA],
    )
    conn.executemany(
        "INSERT INTO sync_audit (table_name, record_id, record_json, sync_status, timestamp) VALUES (?,?,?,?,?)",
        [(s[1], s[2], s[3], s[4], s[5]) for s in _SYNC_AUDIT_DATA],
    )
