#!/usr/bin/env python3
"""
환경 변수 디버깅 스크립트
"""

import os
from dotenv import load_dotenv

# 환경 변수 로드
load_dotenv()

print("🔍 환경 변수 디버깅")
print(f"KTO_API_KEY 원본: {repr(os.getenv('KTO_API_KEY'))}")
print(f"KMA_API_KEY: {repr(os.getenv('KMA_API_KEY'))}")

kto_key = os.getenv('KTO_API_KEY', '')
if kto_key:
    if ',' in kto_key:
        keys = [k.strip() for k in kto_key.split(',') if k.strip()]
        print(f"KTO 키 개수: {len(keys)}")
        for i, key in enumerate(keys):
            print(f"  키 {i+1}: {key[:10]}... (길이: {len(key)})")
    else:
        print(f"KTO 단일 키: {kto_key[:10]}... (길이: {len(kto_key)})")
else:
    print("KTO_API_KEY가 설정되지 않았음")

# 다중 키 매니저 테스트
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.core.multi_api_key_manager import MultiAPIKeyManager, APIProvider

manager = MultiAPIKeyManager()
print(f"\n📊 로드된 KTO 키 개수: {len(manager.api_keys[APIProvider.KTO])}")
print(f"📊 로드된 KMA 키 개수: {len(manager.api_keys[APIProvider.KMA])}")

# 각 키 정보 출력
print("\n🔑 KTO 키 상세 정보:")
for i, key_info in enumerate(manager.api_keys[APIProvider.KTO]):
    print(f"  키 {i}: {key_info.key[:10]}... (활성: {key_info.is_active}, 사용량: {key_info.current_usage}/{key_info.daily_limit})")

print("\n🔑 KMA 키 상세 정보:")
for i, key_info in enumerate(manager.api_keys[APIProvider.KMA]):
    print(f"  키 {i}: {key_info.key[:10]}... (활성: {key_info.is_active}, 사용량: {key_info.current_usage}/{key_info.daily_limit})")

# 활성 키 테스트
print("\n🧪 활성 키 테스트:")
kto_active = manager.get_active_key(APIProvider.KTO)
kma_active = manager.get_active_key(APIProvider.KMA)

print(f"KTO 활성 키: {kto_active.key[:10] + '...' if kto_active else 'None'}")
print(f"KMA 활성 키: {kma_active.key[:10] + '...' if kma_active else 'None'}")

# 통계 정보
stats = manager.get_usage_stats()
print(f"\n📈 통계: 총 {stats['total_keys']}개 키, 활성 {stats['active_keys']}개")