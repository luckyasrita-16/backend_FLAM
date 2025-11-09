import sqlite3
import json
import os
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), "queuectl1.db")

def get_connection():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            command TEXT NOT NULL,
            state TEXT NOT NULL,
            attempts INTEGER NOT NULL,
            max_retries INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dlq (
            id TEXT PRIMARY KEY,
            command TEXT NOT NULL,
            attempts INTEGER NOT NULL,
            max_retries INTEGER NOT NULL,
            failed_reason TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS config (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()

def save_job(job):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR REPLACE INTO jobs 
        (id, command, state, attempts, max_retries, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        job['id'],
        job['command'],
        job['state'],
        job['attempts'],
        job['max_retries'],
        job['created_at'],
        job['updated_at'],
    ))
    conn.commit()
    conn.close()

def get_pending_job():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM jobs WHERE state='pending'
        ORDER BY created_at ASC LIMIT 1
    """)
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None

def update_job_state(job_id, new_state, attempts=None, updated_at=None):
    conn = get_connection()
    cursor = conn.cursor()
    if attempts is None:
        cursor.execute("""
            UPDATE jobs SET state=?, updated_at=? WHERE id=?
        """, (new_state, updated_at or datetime.utcnow().isoformat(), job_id))
    else:
        cursor.execute("""
            UPDATE jobs SET state=?, attempts=?, updated_at=? WHERE id=?
        """, (new_state, attempts, updated_at or datetime.utcnow().isoformat(), job_id))
    conn.commit()
    conn.close()

def list_jobs_by_state(state):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM jobs WHERE state=?
        ORDER BY created_at ASC
    """, (state,))
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]

def move_job_to_dlq(job_id, failed_reason=""):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM jobs WHERE id=?", (job_id,))
    job = cursor.fetchone()
    if not job:
        conn.close()
        return False
    cursor.execute("""
        INSERT OR REPLACE INTO dlq 
        (id, command, attempts, max_retries, failed_reason, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        job['id'],
        job['command'],
        job['attempts'],
        job['max_retries'],
        failed_reason,
        job['created_at'],
        job['updated_at'],
    ))
    cursor.execute("DELETE FROM jobs WHERE id=?", (job_id,))
    conn.commit()
    conn.close()
    return True

def list_dlq_jobs():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM dlq ORDER BY created_at ASC")
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]

def retry_dlq_job(job_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM dlq WHERE id=?", (job_id,))
    dlq_job = cursor.fetchone()
    if not dlq_job:
        conn.close()
        return False
    # Re-insert as pending job with attempts reset
    cursor.execute("""
        INSERT OR REPLACE INTO jobs
        (id, command, state, attempts, max_retries, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        dlq_job['id'],
        dlq_job['command'],
        'pending',
        0,
        dlq_job['max_retries'],
        dlq_job['created_at'],
        datetime.utcnow().isoformat(),
    ))
    cursor.execute("DELETE FROM dlq WHERE id=?", (job_id,))
    conn.commit()
    conn.close()
    return True

def set_config(key, value):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)
    """, (key, value))
    conn.commit()
    conn.close()

def get_config(key, default=None):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM config WHERE key=?", (key,))
    row = cursor.fetchone()
    conn.close()
    return row['value'] if row else default
