import os
import mysql.connector


def _connect():
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", "traffic1234"),
        database=os.getenv("MYSQL_DATABASE", "traffic"),
    )


def insert_count(cctv_name: str, count: int) -> None:
    conn = _connect()
    try:
        cursor = conn.cursor()
        sql = "INSERT INTO vehicle_count (cctv_name, count) VALUES (%s, %s)"
        cursor.execute(sql, (cctv_name, count))
        conn.commit()
    finally:
        conn.close()


def _has_up_down_columns(cursor) -> bool:
    cursor.execute("SHOW COLUMNS FROM vehicle_count LIKE 'up_count'")
    has_up = cursor.fetchone() is not None
    cursor.execute("SHOW COLUMNS FROM vehicle_count LIKE 'down_count'")
    has_down = cursor.fetchone() is not None
    return has_up and has_down


def _has_hybrid_columns(cursor) -> bool:
    cursor.execute("SHOW COLUMNS FROM vehicle_count LIKE 'up_count_hard'")
    return cursor.fetchone() is not None


def insert_batch(rows: list[tuple]) -> None:
    """
    rows: 5튜플 (cctv_name, up_h, down_h, up_s, down_s) 또는
          3튜플 (cctv_name, up_count, down_count) — 하드만 있는 레거시 호출.
    하이브리드 컬럼이 있으면 hard/soft + 합계(up_count/down_count)까지 저장.
    """
    if not rows:
        return

    conn = _connect()
    try:
        cursor = conn.cursor()
        sample = rows[0]
        if len(sample) == 3:
            legacy_rows = rows
            hybrid_rows = None
        elif len(sample) == 5:
            legacy_rows = None
            hybrid_rows = rows
        else:
            raise ValueError("insert_batch rows must be 3- or 5-tuples")

        if hybrid_rows is not None and _has_hybrid_columns(cursor):
            sql = (
                "INSERT INTO vehicle_count ("
                "cctv_name, up_count_hard, down_count_hard, up_count_soft, down_count_soft, "
                "up_count, down_count"
                ") VALUES (%s, %s, %s, %s, %s, %s, %s)"
            )
            exec_rows = []
            for name, uh, dh, us, ds in hybrid_rows:
                uh, dh, us, ds = int(uh), int(dh), int(us), int(ds)
                exec_rows.append(
                    (name, uh, dh, us, ds, uh + us, dh + ds),
                )
            cursor.executemany(sql, exec_rows)
        elif hybrid_rows is not None and _has_up_down_columns(cursor):
            sql = (
                "INSERT INTO vehicle_count (cctv_name, up_count, down_count) "
                "VALUES (%s, %s, %s)"
            )
            exec_rows = [
                (name, int(uh) + int(us), int(dh) + int(ds)) for name, uh, dh, us, ds in hybrid_rows
            ]
            cursor.executemany(sql, exec_rows)
        elif legacy_rows is not None and _has_up_down_columns(cursor):
            sql = (
                "INSERT INTO vehicle_count (cctv_name, up_count, down_count) "
                "VALUES (%s, %s, %s)"
            )
            cursor.executemany(sql, legacy_rows)
        elif hybrid_rows is not None:
            sql = "INSERT INTO vehicle_count (cctv_name, count) VALUES (%s, %s)"
            total_rows = [
                (name, int(uh) + int(dh) + int(us) + int(ds)) for name, uh, dh, us, ds in hybrid_rows
            ]
            cursor.executemany(sql, total_rows)
        else:
            sql = "INSERT INTO vehicle_count (cctv_name, count) VALUES (%s, %s)"
            total_rows = [(name, int(up) + int(down)) for name, up, down in legacy_rows]
            cursor.executemany(sql, total_rows)
        conn.commit()
    finally:
        conn.close()

