"""traffic DB 조회·가공."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import Any


def _total_estimate(row: dict[str, Any]) -> int:
    c = row.get("count")
    if c is not None:
        try:
            return int(c)
        except (TypeError, ValueError):
            pass
    up = row.get("up_count") or 0
    down = row.get("down_count") or 0
    try:
        return int(up) + int(down)
    except (TypeError, ValueError):
        return 0


def list_sites(cur: Any) -> list[str]:
    cur.execute(
        """
        SELECT DISTINCT cctv_name FROM (
            SELECT cctv_name FROM vehicle_count WHERE cctv_name IS NOT NULL AND cctv_name <> ''
            UNION
            SELECT cctv_name FROM vehicle_count_hourly WHERE cctv_name IS NOT NULL AND cctv_name <> ''
        ) u
        ORDER BY cctv_name
        """
    )
    rows = cur.fetchall() or []
    return [r["cctv_name"] for r in rows if r.get("cctv_name")]


def latest_per_site(cur: Any) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT vc.id, vc.cctv_name, vc.count, vc.up_count, vc.down_count,
               vc.up_count_hard, vc.down_count_hard, vc.up_count_soft, vc.down_count_soft,
               vc.created_at
        FROM vehicle_count vc
        INNER JOIN (
            SELECT cctv_name, MAX(id) AS max_id
            FROM vehicle_count
            WHERE cctv_name IS NOT NULL AND cctv_name <> ''
            GROUP BY cctv_name
        ) t ON vc.cctv_name = t.cctv_name AND vc.id = t.max_id
        ORDER BY vc.cctv_name
        """
    )
    rows = cur.fetchall() or []
    out: list[dict[str, Any]] = []
    for r in rows:
        item = dict(r)
        item["total_estimate"] = _total_estimate(item)
        out.append(item)
    return out


def fetch_raw_counts(
    cur: Any,
    *,
    cctv_name: str | None,
    start: datetime | None,
    end: datetime | None,
    limit: int,
    offset: int,
) -> list[dict[str, Any]]:
    where: list[str] = []
    params: list[Any] = []
    if cctv_name:
        where.append("cctv_name = %s")
        params.append(cctv_name)
    if start:
        where.append("created_at >= %s")
        params.append(start)
    if end:
        where.append("created_at < %s")
        params.append(end)
    cond = (" WHERE " + " AND ".join(where)) if where else ""
    sql = (
        "SELECT id, cctv_name, count, up_count, down_count, "
        "up_count_hard, down_count_hard, up_count_soft, down_count_soft, created_at "
        "FROM vehicle_count"
        f"{cond} ORDER BY id DESC LIMIT %s OFFSET %s"
    )
    params.extend([limit, offset])
    cur.execute(sql, tuple(params))
    return list(cur.fetchall() or [])


def fetch_hourly(
    cur: Any,
    *,
    cctv_name: str | None,
    start: datetime | None,
    end: datetime | None,
    limit: int,
    offset: int,
) -> list[dict[str, Any]]:
    where: list[str] = []
    params: list[Any] = []
    if cctv_name:
        where.append("cctv_name = %s")
        params.append(cctv_name)
    if start:
        where.append("hour_bucket >= %s")
        params.append(start)
    if end:
        where.append("hour_bucket < %s")
        params.append(end)
    cond = (" WHERE " + " AND ".join(where)) if where else ""
    sql = (
        "SELECT id, hour_bucket, cctv_name, up_start, up_end, down_start, down_end, "
        "up_delta, down_delta, event_count, last_created_at, updated_at "
        "FROM vehicle_count_hourly"
        f"{cond} ORDER BY hour_bucket DESC, cctv_name ASC LIMIT %s OFFSET %s"
    )
    params.extend([limit, offset])
    cur.execute(sql, tuple(params))
    return list(cur.fetchall() or [])


def daily_summary(
    cur: Any,
    *,
    cctv_name: str | None,
    day_start: date,
    day_end: date,
) -> list[dict[str, Any]]:
    """일별·지점별로 원본 샘플 수와 up/down 변화량(맥스−민) 추정."""
    where_site = ""
    params: list[Any] = [day_start, day_end]
    if cctv_name:
        where_site = " AND cctv_name = %s "
        params.append(cctv_name)
    sql = f"""
        SELECT
            DATE(created_at) AS day,
            cctv_name,
            COUNT(*) AS samples,
            MAX(COALESCE(up_count, 0)) AS up_max,
            MAX(COALESCE(down_count, 0)) AS down_max,
            GREATEST(MAX(COALESCE(up_count, 0)) - MIN(COALESCE(up_count, 0)), 0) AS up_delta_est,
            GREATEST(MAX(COALESCE(down_count, 0)) - MIN(COALESCE(down_count, 0)), 0) AS down_delta_est
        FROM vehicle_count
        WHERE created_at >= %s
          AND created_at < DATE_ADD(%s, INTERVAL 1 DAY)
          AND cctv_name IS NOT NULL AND cctv_name <> ''
          {where_site}
        GROUP BY DATE(created_at), cctv_name
        ORDER BY day DESC, cctv_name
    """
    cur.execute(sql, tuple(params))
    rows = cur.fetchall() or []
    out = []
    for r in rows:
        item = dict(r)
        day_val = item["day"]
        if hasattr(day_val, "isoformat"):
            item["day"] = datetime.combine(day_val, time.min)
        out.append(item)
    return out


def _normalize_naive_hour(dt: datetime) -> datetime:
    if dt.tzinfo is not None:
        dt = dt.replace(tzinfo=None)
    return dt.replace(minute=0, second=0, microsecond=0)


def fetch_hourly_matrix(
    cur: Any,
    *,
    day: date,
    hour_start: int,
    hour_end: int,
    cctv_name: str | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """
    특정 일자의 hour_start~hour_end(포함) 시각 버킷별로 vehicle_count_hourly 행을 묶음.
    반환: (grouped_by_hour, flat_rows)
    """
    if hour_start > hour_end:
        hour_start, hour_end = hour_end, hour_start
    if not (0 <= hour_start <= 23 and 0 <= hour_end <= 23):
        raise ValueError("hour_start/to_hour must be 0..23")

    range_start = datetime.combine(day, time(hour_start, 0))
    range_end_exclusive = datetime.combine(day, time(hour_end, 0)) + timedelta(hours=1)

    where = ["hour_bucket >= %s", "hour_bucket < %s"]
    params: list[Any] = [range_start, range_end_exclusive]
    if cctv_name:
        where.append("cctv_name = %s")
        params.append(cctv_name)

    sql = f"""
        SELECT hour_bucket, cctv_name, up_delta, down_delta
        FROM vehicle_count_hourly
        WHERE {" AND ".join(where)}
        ORDER BY hour_bucket ASC, cctv_name ASC
    """
    cur.execute(sql, tuple(params))
    db_rows = list(cur.fetchall() or [])

    bucket_keys: list[datetime] = []
    h = hour_start
    while h <= hour_end:
        bucket_keys.append(datetime.combine(day, time(h, 0)))
        h += 1

    cells_by_bucket: dict[datetime, list[dict[str, Any]]] = {bk: [] for bk in bucket_keys}

    flat: list[dict[str, Any]] = []
    for r in db_rows:
        hb = _normalize_naive_hour(r["hour_bucket"])
        if hb not in cells_by_bucket:
            continue
        cell = {
            "cctv_name": str(r["cctv_name"]),
            "up_traffic": int(r["up_delta"] or 0),
            "down_traffic": int(r["down_delta"] or 0),
        }
        cells_by_bucket[hb].append(cell)
        flat.append(
            {
                "hour_bucket": hb,
                "cctv_name": cell["cctv_name"],
                "up_traffic": cell["up_traffic"],
                "down_traffic": cell["down_traffic"],
            }
        )

    grouped = [{"hour_bucket": bk, "sites": cells_by_bucket[bk]} for bk in bucket_keys]
    return grouped, flat


def parse_day(s: str | None, default_offset_days: int = 7) -> tuple[date, date]:
    """쿼리 파라미터용 기본 구간: 최근 N일."""
    today = datetime.utcnow().date()
    if not s:
        start = today - timedelta(days=default_offset_days)
        return start, today
    parts = s.split(",", 1)
    if len(parts) == 2:
        d0 = date.fromisoformat(parts[0].strip())
        d1 = date.fromisoformat(parts[1].strip())
        return min(d0, d1), max(d0, d1)
    d0 = date.fromisoformat(s.strip())
    return d0, d0
