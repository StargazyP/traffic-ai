-- 2026-04-28: vehicle_count 시간 단위 집계 테이블/인덱스 마이그레이션
USE traffic;

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
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci;

CREATE INDEX idx_vehicle_count_created_at_name
  ON vehicle_count (created_at, cctv_name);
