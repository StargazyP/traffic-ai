USE traffic;

-- vehicle_count 테이블에 상/하행 컬럼 추가
-- 주의: MySQL 8.0 환경 호환을 위해 컬럼을 각각 추가합니다.
ALTER TABLE vehicle_count
    ADD COLUMN up_count INT NOT NULL DEFAULT 0;

ALTER TABLE vehicle_count
    ADD COLUMN down_count INT NOT NULL DEFAULT 0;

-- 기존 단일 count 데이터가 있다면 down_count로 백필(보수적 기본값)
UPDATE vehicle_count
SET down_count = COALESCE(count, 0)
WHERE COALESCE(up_count, 0) = 0
  AND COALESCE(down_count, 0) = 0
  AND count IS NOT NULL;
