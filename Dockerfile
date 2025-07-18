# 날씨 기반 여행지 추천 서비스 배치 시스템 Dockerfile

FROM python:3.11-slim

# 작업 디렉토리 설정
WORKDIR /app

# 시스템 패키지 업데이트 및 필요한 도구 설치
RUN apt-get update && apt-get install -y \
  curl \
  vim \
  cron \
  && rm -rf /var/lib/apt/lists/*

# Python 의존성 파일 복사
COPY requirements.txt .

# Python 패키지 설치
RUN pip install --no-cache-dir -r requirements.txt

# 애플리케이션 코드 복사
COPY . /app

# 로그 디렉토리 생성
RUN mkdir -p /app/logs /app/data/raw /app/data/processed /app/data/sample

# 환경 변수 설정
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# 포트 노출 (모니터링용 및 API 서버용)
EXPOSE 9090

# 헬스체크 (API 서버 헬스체크)
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:9090/health || exit 1

# 실행 명령 (API 서버 포함)
CMD ["python", "main_with_api.py"]
