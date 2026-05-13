import os

from yolo_mysql_counter import run_counter


if __name__ == "__main__":
    cctv_url = os.getenv("CCTV_URL", "")
    cctv_name = os.getenv("CCTV_NAME", "서울CCTV1")
    if not cctv_url:
        raise SystemExit("환경변수 CCTV_URL에 m3u8 URL을 설정하세요.")
    run_counter(cctv_url, cctv_name)

