# Dockerfile
# 기존 Airflow 이미지 사용
FROM apache/airflow:2.9.3

# root 유저로 전환하여 시스템 패키지 설치
USER root

# 시스템 패키지 설치
RUN apt-get update && apt-get install -y --no-install-recommends \
    # playwright 실행에 필요한 종속성 설치
    sudo \
    # 필요한 다른 apt 패키지 추가 (예: vim)
    vim \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# airflow 유저로 전환하여 Python 패키지 설치
USER airflow

COPY requirements.txt .
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r requirements.txt

# airflow 유저로 playwright Python 패키지 설치
RUN pip install playwright

# root로 전환하여 playwright 브라우저와 시스템 의존성 설치
USER root
# RUN python -m playwright install --with-deps
RUN python -m playwright install-deps chromium

# 파일 복사
COPY src/ /opt/airflow/src/
COPY data/ /opt/airflow/data/

# playwright 브라우저들의 권한 설정 (airflow 유저가 사용할 수 있도록)
RUN chown -R airflow:root /home/airflow/.cache

# 최종적으로 airflow 유저로 전환
USER airflow