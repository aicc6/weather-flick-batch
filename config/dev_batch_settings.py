"""
개발 단계 배치 설정
서비스 준비를 위한 적극적인 데이터 수집 스케줄
"""

# 개발 단계 스케줄 (더 자주 실행)
DEV_BATCH_SCHEDULE = {
    # 관광지 데이터 수집을 더 적극적으로
    "comprehensive_tourism_sync": {
        "trigger": "cron",
        "day_of_week": "sun,wed",  # 일요일, 수요일 (주 2회)
        "hour": 2,
        "minute": 0,
        "priority": "HIGH",
        "timeout": 6 * 3600,  # 6시간으로 연장
        "retry_attempts": 3
    },
    
    "incremental_tourism_sync": {
        "trigger": "cron", 
        "hour": 3,
        "minute": 0,
        "priority": "HIGH",  # HIGH로 승격
        "timeout": 2 * 3600,  # 2시간으로 연장
        "retry_attempts": 3
    },
    
    # 데이터 품질 검사도 더 자주
    "data_quality_check": {
        "trigger": "cron",
        "hour": "6,14,22",  # 하루 3번 (6시, 14시, 22시)
        "minute": 0,
        "priority": "HIGH",
        "timeout": 30 * 60,
        "retry_attempts": 2
    },
    
    # 추천 점수 계산도 더 자주
    "recommendation_update": {
        "trigger": "cron",
        "hour": "5,13,21",  # 하루 3번
        "minute": 0,
        "priority": "HIGH",
        "timeout": 45 * 60,
        "retry_attempts": 2
    }
}

# 개발 단계 API 설정 (더 적극적인 수집)
DEV_API_SETTINGS = {
    "kto_batch_size": 1000,      # 배치 크기 증가
    "kto_max_pages": 100,        # 페이지 수 증가
    "kto_concurrent_requests": 5, # 동시 요청 수 증가
    "enable_detailed_collection": True,  # 상세 정보 수집 활성화
    "collect_images": True,      # 이미지 데이터 수집
    "store_raw_data": True       # 원본 데이터 보존
}

# 개발 단계 우선순위 지역 (인기 관광지 우선)
DEV_PRIORITY_REGIONS = [
    "1",   # 서울
    "39",  # 제주
    "6",   # 부산  
    "31",  # 경기
    "32",  # 강원
    "35",  # 경북 (경주)
    "36",  # 경남
    "38",  # 전남 (여수)
    "37",  # 전북 (전주)
    "33",  # 충북
    "34"   # 충남
]

# 개발 단계 우선순위 컨텐츠 타입
DEV_PRIORITY_CONTENT_TYPES = [
    "12",  # 관광지 (최우선)
    "32",  # 숙박
    "39",  # 음식점
    "15",  # 축제공연행사
    "14",  # 문화시설
    "25",  # 여행코스
    "28",  # 레포츠
    "38"   # 쇼핑
]