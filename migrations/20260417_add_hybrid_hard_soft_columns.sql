USE traffic;

-- Line-cross(hard) + Flow 보정(soft) 분리 저장
ALTER TABLE vehicle_count
    ADD COLUMN up_count_hard INT NOT NULL DEFAULT 0;

ALTER TABLE vehicle_count
    ADD COLUMN down_count_hard INT NOT NULL DEFAULT 0;

ALTER TABLE vehicle_count
    ADD COLUMN up_count_soft INT NOT NULL DEFAULT 0;

ALTER TABLE vehicle_count
    ADD COLUMN down_count_soft INT NOT NULL DEFAULT 0;
