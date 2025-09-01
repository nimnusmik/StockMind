# 기존 Airflow 이미지 사용
FROM apache/airflow:2.9.3

# root 유저로 전환하여 시스템 패키지 설치
USER root

# 시스템 패키지 및 playwright 시스템 의존성 설치
RUN apt-get update && apt-get install -y --no-install-recommends \
    sudo \
    vim \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# airflow 유저로 전환하여 Python 패키지 설치
USER airflow

# requirements.txt 복사 및 설치
COPY requirements.txt .
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r requirements.txt
RUN pip check

# playwright Python 패키지 설치
RUN pip install playwright

# playwright 시스템 의존성을 root로 설치
USER root
RUN python -m playwright install-deps chromium

# ⚠️ 중요: airflow 유저로 다시 전환 후 브라우저 설치
USER airflow
RUN python -m playwright install chromium

# 파일 복사 (root 권한 필요)
USER root
COPY src/ /opt/airflow/src/
COPY data/ /opt/airflow/data/
RUN chown -R airflow:root /opt/airflow/src/ /opt/airflow/data/

# 캐시 정리
RUN apt-get clean && rm -rf /root/.cache/pip

# 최종적으로 airflow 유저로 전환
USER airflow