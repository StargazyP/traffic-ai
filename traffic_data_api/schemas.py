from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    status: str = "ok"
    database: str = "reachable"


class SiteItem(BaseModel):
    cctv_name: str


class LatestCountItem(BaseModel):
    id: int
    cctv_name: str | None = None
    count: int | None = None
    up_count: int | None = None
    down_count: int | None = None
    up_count_hard: int | None = None
    down_count_hard: int | None = None
    up_count_soft: int | None = None
    down_count_soft: int | None = None
    total_estimate: int = Field(
        ...,
        description="count 컬럼 또는 up+down 중 유효한 값으로 추정한 합계성 지표",
    )
    created_at: datetime | None = None


class RawCountItem(BaseModel):
    id: int
    cctv_name: str | None = None
    count: int | None = None
    up_count: int | None = None
    down_count: int | None = None
    up_count_hard: int | None = None
    down_count_hard: int | None = None
    up_count_soft: int | None = None
    down_count_soft: int | None = None
    created_at: datetime | None = None


class HourlyItem(BaseModel):
    id: int
    hour_bucket: datetime
    cctv_name: str
    up_start: int
    up_end: int
    down_start: int
    down_end: int
    up_delta: int
    down_delta: int
    event_count: int
    last_created_at: datetime | None = None
    updated_at: datetime | None = None


class DailySummaryItem(BaseModel):
    day: datetime
    cctv_name: str
    samples: int
    up_max: int
    down_max: int
    up_delta_est: int = Field(..., description="해당 일자 샘플 중 up_count 최대−최소(≥0)")
    down_delta_est: int = Field(..., description="해당 일자 샘플 중 down_count 최대−최소(≥0)")


class PaginatedMeta(BaseModel):
    limit: int
    offset: int
    returned: int


class RawCountsResponse(BaseModel):
    meta: PaginatedMeta
    items: list[RawCountItem]


class HourlyMatrixSiteCell(BaseModel):
    """한 시간대 안의 한 지점."""

    cctv_name: str
    up_traffic: int = Field(..., description="상행(시간 롤업 up_delta)")
    down_traffic: int = Field(..., description="하행(시간 롤업 down_delta)")


class HourlyMatrixHourRow(BaseModel):
    """한 시간(정각 버킷)을 한 행으로 — 열 방향으로 여러 CCTV 셀이 붙는 형태."""

    hour_bucket: datetime
    sites: list[HourlyMatrixSiteCell]


class HourlyMatrixFlatRow(BaseModel):
    """표·CSV용 평탄 행: 시간 × 지점."""

    hour_bucket: datetime
    cctv_name: str
    up_traffic: int = Field(..., description="상행")
    down_traffic: int = Field(..., description="하행")


class HourlyMatrixResponse(BaseModel):
    """날짜·시간 구간을 행(시간) 단위로 묶은 매트릭스."""

    date: str = Field(..., description="조회 일자 YYYY-MM-DD")
    from_hour: int
    to_hour: int
    grouped_by_hour: list[HourlyMatrixHourRow]
    flat: list[HourlyMatrixFlatRow] = Field(
        default_factory=list,
        description="동일 데이터를 (시간, 지점) 행으로 펼친 목록",
    )
