"""MySQL 연결 — traffic-ai 의 db_mysql 와 동일한 환경변수."""

from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Generator

import mysql.connector
from mysql.connector.connection import MySQLConnection


def connect() -> MySQLConnection:
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", "changeme"),
        database=os.getenv("MYSQL_DATABASE", "traffic"),
        charset="utf8mb4",
        collation="utf8mb4_unicode_ci",
        use_unicode=True,
    )


@contextmanager
def get_cursor(*, dictionary: bool = True) -> Generator:
    conn = connect()
    try:
        cur = conn.cursor(dictionary=dictionary)
        yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
