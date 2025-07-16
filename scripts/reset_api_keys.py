#!/usr/bin/env python3
"""
API 키 재활성화 스크립트
"""

import json

# API 키 캐시 파일 경로
cache_file = "/Users/sl/Repository/aicc6/weather-flick-batch/data/cache/api_key_cache.json"

# 파일 읽기
with open(cache_file, 'r') as f:
    data = json.load(f)

# 모든 API 키 재활성화
for provider, provider_data in data['providers'].items():
    for key in provider_data['keys']:
        key['is_active'] = True
        key['error_count'] = 0

# 파일 저장
with open(cache_file, 'w') as f:
    json.dump(data, f, indent=2, ensure_ascii=False)

print("모든 API 키가 재활성화되었습니다.")