"""
traffic-ai MySQL 적재 데이터를 가공·조회하는 전용 API.

실행 예:
  cd traffic-ai/traffic_data_api && uvicorn main:app --host 0.0.0.0 --port 8001

환경변수는 traffic-ai 앱과 동일: MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE
"""

from __future__ import annotations

from datetime import date, datetime

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from db import get_cursor
from queries import (
    daily_summary,
    fetch_hourly,
    fetch_hourly_matrix,
    fetch_raw_counts,
    latest_per_site,
    list_sites,
    parse_day,
)
from schemas import (
    DailySummaryItem,
    HealthResponse,
    HourlyItem,
    HourlyMatrixFlatRow,
    HourlyMatrixHourRow,
    HourlyMatrixResponse,
    HourlyMatrixSiteCell,
    LatestCountItem,
    PaginatedMeta,
    RawCountItem,
    RawCountsResponse,
    SiteItem,
)

app = FastAPI(
    title="Traffic Data API",
    description="traffic-ai 가 MySQL에 저장한 차량 카운트를 가공해 제공합니다.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    try:
        with get_cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"database_unavailable: {exc}") from exc
    return HealthResponse()


@app.get("/api/v1/sites", response_model=list[SiteItem])
def api_sites() -> list[SiteItem]:
    with get_cursor() as cur:
        names = list_sites(cur)
    return [SiteItem(cctv_name=n) for n in names]


@app.get("/api/v1/counts/latest", response_model=list[LatestCountItem])
def api_counts_latest() -> list[LatestCountItem]:
    with get_cursor() as cur:
        rows = latest_per_site(cur)
    return [LatestCountItem.model_validate(r) for r in rows]


@app.get("/api/v1/counts/raw", response_model=RawCountsResponse)
def api_counts_raw(
    cctv_name: str | None = Query(None, description="지점 이름 필터"),
    start: datetime | None = Query(None, description="created_at 이상 (UTC 권장)"),
    end: datetime | None = Query(None, description="created_at 미만"),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> RawCountsResponse:
    with get_cursor() as cur:
        rows = fetch_raw_counts(
            cur,
            cctv_name=cctv_name,
            start=start,
            end=end,
            limit=limit,
            offset=offset,
        )
    items = [RawCountItem.model_validate(r) for r in rows]
    return RawCountsResponse(
        meta=PaginatedMeta(limit=limit, offset=offset, returned=len(items)),
        items=items,
    )


@app.get("/api/v1/counts/hourly", response_model=list[HourlyItem])
def api_counts_hourly(
    cctv_name: str | None = None,
    start: datetime | None = Query(None, description="hour_bucket 이상"),
    end: datetime | None = Query(None, description="hour_bucket 미만"),
    limit: int = Query(168, ge=1, le=2000, description="기본 약 7일치(시간×지점 수에 따라 다름)"),
    offset: int = Query(0, ge=0),
) -> list[HourlyItem]:
    with get_cursor() as cur:
        rows = fetch_hourly(
            cur,
            cctv_name=cctv_name,
            start=start,
            end=end,
            limit=limit,
            offset=offset,
        )
    return [HourlyItem.model_validate(r) for r in rows]


@app.get(
    "/api/v1/matrix/hourly-by-hour",
    response_model=HourlyMatrixResponse,
    summary="시간=행, CCTV별 상·하행=열(셀 묶음)",
)
def api_matrix_hourly_by_hour(
    day: str = Query(..., description="조회 일자 YYYY-MM-DD (예: 2026-05-13)"),
    from_hour: int = Query(12, ge=0, le=23, description="시작 시(포함), 0~23"),
    to_hour: int = Query(15, ge=0, le=23, description="끝 시(포함), 0~23"),
    cctv_name: str | None = Query(None, description="지점 이름(미지정이면 해당 일자·구간 전체 지점)"),
) -> HourlyMatrixResponse:
    """
    `vehicle_count_hourly` 기준으로, 지정한 일의 각 정각 시간대를 한 행으로 묶습니다.
    각 행의 `sites[]`에 CCTV 이름·상행(up_delta)·하행(down_delta)이 들어 갑니다.
    데이터가 없는 시간대도 `sites: []`로 행이 채워 집니다.
    """
    try:
        d = date.fromisoformat(day.strip())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="invalid date, use YYYY-MM-DD") from exc

    hf, ht = (from_hour, to_hour) if from_hour <= to_hour else (to_hour, from_hour)

    try:
        with get_cursor() as cur:
            grouped_raw, flat_raw = fetch_hourly_matrix(
                cur,
                day=d,
                hour_start=hf,
                hour_end=ht,
                cctv_name=cctv_name,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    grouped = [
        HourlyMatrixHourRow(
            hour_bucket=g["hour_bucket"],
            sites=[HourlyMatrixSiteCell.model_validate(s) for s in g["sites"]],
        )
        for g in grouped_raw
    ]
    flat = [HourlyMatrixFlatRow.model_validate(r) for r in flat_raw]

    return HourlyMatrixResponse(
        date=d.isoformat(),
        from_hour=hf,
        to_hour=ht,
        grouped_by_hour=grouped,
        flat=flat,
    )


@app.get("/api/v1/summary/daily", response_model=list[DailySummaryItem])
def api_summary_daily(
    cctv_name: str | None = None,
    date_range: str | None = Query(
        None,
        alias="range",
        description="단일 일자 YYYY-MM-DD 또는 시작,종료 YYYY-MM-DD,YYYY-MM-DD (미지정 시 최근 7일)",
    ),
) -> list[DailySummaryItem]:
    d0, d1 = parse_day(date_range, default_offset_days=7)
    with get_cursor() as cur:
        rows = daily_summary(cur, cctv_name=cctv_name, day_start=d0, day_end=d1)
    return [DailySummaryItem.model_validate(r) for r in rows]
