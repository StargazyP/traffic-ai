# 2026-04-28: 시간 단위 롤업(vehicle_count_hourly) + 원본 보존 정책(retention) 추가.
# 2026-05-14: 롤업 경계를 세션 타임존(기본 KST +09:00) 기준 정각으로 맞춤 + 스케줄러 정렬.
import datetime
import os
import threading
from zoneinfo import ZoneInfo

import mysql.connector


def _session_timezone_sql() -> str:
    """mysql 세션 time_zone (OFFSET 형태는 time_zone 테이블 불필요)."""
    return os.getenv("DB_ROLLUP_TIMEZONE", "+09:00").strip() or "+09:00"


def _connect():
    conn = mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", "changeme"),
        database=os.getenv("MYSQL_DATABASE", "traffic"),
        charset="utf8mb4",
        collation="utf8mb4_unicode_ci",
        use_unicode=True,
    )
    tz = _session_timezone_sql()
    cur = conn.cursor()
    cur.execute("SET SESSION time_zone = %s", (tz,))
    cur.close()
    return conn


def _seconds_until_next_hour_boundary() -> float:
    """한국(또는 DB_ROLLUP_CLOCK_TZ) 달력 기준 다음 정각까지 초."""
    name = os.getenv("DB_ROLLUP_CLOCK_TZ", "Asia/Seoul").strip() or "Asia/Seoul"
    try:
        tz = ZoneInfo(name)
    except Exception:
        tz = ZoneInfo("Asia/Seoul")
    now = datetime.datetime.now(tz)
    next_top = now.replace(minute=0, second=0, microsecond=0) + datetime.timedelta(hours=1)
    return max(1.0, (next_top - now).total_seconds())


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


def _ensure_rollup_schema(cursor) -> None:
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS vehicle_count_hourly (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            hour_bucket DATETIME NOT NULL,
            cctv_name VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
            up_start INT NOT NULL DEFAULT 0,
            up_end INT NOT NULL DEFAULT 0,
            down_start INT NOT NULL DEFAULT 0,
            down_end INT NOT NULL DEFAULT 0,
            up_delta INT NOT NULL DEFAULT 0,
            down_delta INT NOT NULL DEFAULT 0,
            event_count INT NOT NULL DEFAULT 0,
            last_created_at DATETIME NULL,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uq_vehicle_count_hourly_bucket_name (hour_bucket, cctv_name),
            KEY idx_vehicle_count_hourly_name_bucket (cctv_name, hour_bucket)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
    )
    cursor.execute(
        """
        SELECT COUNT(1)
        FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = 'vehicle_count'
          AND index_name = 'idx_vehicle_count_created_at_name'
        """
    )
    exists = int(cursor.fetchone()[0] or 0)
    if exists == 0:
        cursor.execute(
            """
            CREATE INDEX idx_vehicle_count_created_at_name
            ON vehicle_count (created_at, cctv_name)
            """
        )


def compress_vehicle_count_hourly(retention_hours: int = 72) -> tuple[int, int]:
    """
    원본 vehicle_count를 시간 단위 집계(vehicle_count_hourly)로 압축.
    - hour_bucket: 세션 타임존(DB_ROLLUP_TIMEZONE, 기본 +09:00) 기준 매 시 정각
      (예: 5월 14일 12:00, 13:00, 14:00 … 하루 24구간 중 데이터가 있는 구간만 행 생성).
    - 집계 대상: '현재 시각이 속한 시간' 시작 이전까지 확정된 원본 행
    - 보존 정책: retention_hours보다 오래된 원본 삭제
    Returns: (집계 INSERT 영향 행 수 근사, 삭제 행 수)
    """
    conn = _connect()
    try:
        cursor = conn.cursor()
        _ensure_rollup_schema(cursor)

        cursor.execute(
            """
            INSERT INTO vehicle_count_hourly (
                hour_bucket,
                cctv_name,
                up_start,
                up_end,
                down_start,
                down_end,
                up_delta,
                down_delta,
                event_count,
                last_created_at
            )
            SELECT
                DATE_FORMAT(created_at, '%Y-%m-%d %H:00:00') AS hour_bucket,
                cctv_name,
                MIN(COALESCE(up_count, 0)) AS up_start,
                MAX(COALESCE(up_count, 0)) AS up_end,
                MIN(COALESCE(down_count, 0)) AS down_start,
                MAX(COALESCE(down_count, 0)) AS down_end,
                GREATEST(MAX(COALESCE(up_count, 0)) - MIN(COALESCE(up_count, 0)), 0) AS up_delta,
                GREATEST(MAX(COALESCE(down_count, 0)) - MIN(COALESCE(down_count, 0)), 0) AS down_delta,
                COUNT(*) AS event_count,
                MAX(created_at) AS last_created_at
            FROM vehicle_count
            WHERE created_at < DATE_FORMAT(NOW(), '%Y-%m-%d %H:00:00')
            GROUP BY DATE_FORMAT(created_at, '%Y-%m-%d %H:00:00'), cctv_name
            ON DUPLICATE KEY UPDATE
                up_start = VALUES(up_start),
                up_end = VALUES(up_end),
                down_start = VALUES(down_start),
                down_end = VALUES(down_end),
                up_delta = VALUES(up_delta),
                down_delta = VALUES(down_delta),
                event_count = VALUES(event_count),
                last_created_at = VALUES(last_created_at)
            """
        )
        aggregated_rows = cursor.rowcount if cursor.rowcount is not None else 0

        retention = max(1, int(retention_hours))
        cursor.execute(
            """
            DELETE FROM vehicle_count
            WHERE created_at < (NOW() - INTERVAL %s HOUR)
            """,
            (retention,),
        )
        deleted_rows = cursor.rowcount if cursor.rowcount is not None else 0
        conn.commit()
        return aggregated_rows, deleted_rows
    finally:
        conn.close()


def run_hourly_compression_loop(
    stop_event: threading.Event,
    interval_seconds: int = 3600,
    retention_hours: int = 72,
    align_to_clock: bool | None = None,
) -> None:
    """
    백그라운드 스케줄러: 시간 단위 압축 실행.
    - DB_HOURLY_ALIGN_TO_CLOCK=1(기본): 매 시 정각(DB_ROLLUP_CLOCK_TZ) 직후에 다시 실행되도록 대기.
    - 0이면 DB_HOURLY_COMPRESSION_INTERVAL_SEC마다 실행(기존 방식).
    """
    if align_to_clock is None:
        align_to_clock = os.getenv("DB_HOURLY_ALIGN_TO_CLOCK", "1").strip().lower() in {
            "1",
            "true",
            "yes",
        }
    interval = max(60, int(interval_seconds))
    tz_note = _session_timezone_sql()
    clock_tz = os.getenv("DB_ROLLUP_CLOCK_TZ", "Asia/Seoul").strip() or "Asia/Seoul"
    print(
        f"[DB ROLLUP] 스케줄: 세션 TZ={tz_note}, 정렬 TZ={clock_tz}, 정각맞춤={align_to_clock}",
        flush=True,
    )
    while not stop_event.is_set():
        try:
            agg, deleted = compress_vehicle_count_hourly(retention_hours=retention_hours)
            print(f"[DB ROLLUP] 집계 실행 완료 (영향≈{agg}행, 원본삭제={deleted}행)", flush=True)
        except Exception as exc:
            print("[DB ROLLUP ERROR]", exc)
        if align_to_clock:
            wait_sec = _seconds_until_next_hour_boundary()
            print(f"[DB ROLLUP] 다음 정각 롤업까지 약 {wait_sec:.0f}s", flush=True)
        else:
            wait_sec = float(interval)
        stop_event.wait(wait_sec)

