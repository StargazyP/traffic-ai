-- 2026-04-28: vehicle_count_hourly(시간 단위 집계) 및 인덱스 추가
CREATE DATABASE IF NOT EXISTS traffic
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE traffic;

CREATE TABLE IF NOT EXISTS vehicle_count (
    id INT AUTO_INCREMENT PRIMARY KEY,
    cctv_name VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    count INT,
    up_count_hard INT NOT NULL DEFAULT 0,
    down_count_hard INT NOT NULL DEFAULT 0,
    up_count_soft INT NOT NULL DEFAULT 0,
    down_count_soft INT NOT NULL DEFAULT 0,
    up_count INT NOT NULL DEFAULT 0,
    down_count INT NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci;

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

