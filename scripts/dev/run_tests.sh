#!/bin/bash

# 테스트 실행 스크립트

set -e  # 오류 발생 시 스크립트 중단

echo "=== 날씨 기반 여행지 추천 서비스 배치 시스템 테스트 ==="

# 프로젝트 루트로 이동
cd "$(dirname "$0")/.."

# 가상환경 활성화 (있는 경우)
if [ -d "venv" ]; then
    echo "가상환경 활성화..."
    source venv/bin/activate
fi

# 의존성 설치 확인
echo "의존성 확인 중..."
pip install -r requirements.txt

# 환경 변수 설정
export PYTHONPATH="$(pwd):$PYTHONPATH"
export ENVIRONMENT=test

# 단위 테스트 실행
echo "단위 테스트 실행 중..."
python -m pytest tests/unit/ -v --tb=short

# 통합 테스트 실행
echo "통합 테스트 실행 중..."
python -m pytest tests/integration/ -v --tb=short

# 전체 테스트 커버리지
echo "테스트 커버리지 생성 중..."
python -m pytest tests/ --cov=weather_flick_batch --cov-report=html --cov-report=term

# 코드 스타일 검사
echo "코드 스타일 검사 중..."
flake8 weather_flick_batch/ --max-line-length=100 --ignore=E203,W503

# 타입 체크
echo "타입 체크 중..."
mypy weather_flick_batch/ --ignore-missing-imports

echo "=== 모든 테스트 완료 ==="