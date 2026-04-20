CREATE DATABASE IF NOT EXISTS traffic;

USE traffic;

CREATE TABLE IF NOT EXISTS vehicle_count (
    id INT AUTO_INCREMENT PRIMARY KEY,
    cctv_name VARCHAR(100),
    count INT,
    up_count_hard INT NOT NULL DEFAULT 0,
    down_count_hard INT NOT NULL DEFAULT 0,
    up_count_soft INT NOT NULL DEFAULT 0,
    down_count_soft INT NOT NULL DEFAULT 0,
    up_count INT NOT NULL DEFAULT 0,
    down_count INT NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

