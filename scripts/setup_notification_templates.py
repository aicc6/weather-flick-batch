"""
기본 알림 템플릿 설정 스크립트
"""

import asyncio
import aiohttp
import json

API_URL = "http://localhost:9090/api/batch"
API_KEY = "batch-api-secret-key"

# 기본 템플릿 정의
DEFAULT_TEMPLATES = [
    # 이메일 템플릿
    {
        "event": "job_started",
        "channel": "email",
        "subject_template": "[Weather Flick] 배치 작업 시작: $job_type",
        "message_template": """
배치 작업이 시작되었습니다.

작업 ID: $job_id
작업 유형: $job_type
시작 시간: $started_at

Weather Flick Batch System
""",
        "level": "info",
        "variables": ["job_id", "job_type", "started_at"]
    },
    {
        "event": "job_completed",
        "channel": "email",
        "subject_template": "[Weather Flick] 배치 작업 완료: $job_type",
        "message_template": """
배치 작업이 성공적으로 완료되었습니다.

작업 ID: $job_id
작업 유형: $job_type
시작 시간: $started_at
완료 시간: $completed_at
상태: $status

Weather Flick Batch System
""",
        "level": "info",
        "variables": ["job_id", "job_type", "started_at", "completed_at", "status"]
    },
    {
        "event": "job_failed",
        "channel": "email",
        "subject_template": "[Weather Flick] 배치 작업 실패: $job_type",
        "message_template": """
배치 작업이 실패했습니다.

작업 ID: $job_id
작업 유형: $job_type
시작 시간: $started_at
실패 시간: $completed_at
에러 메시지: $error_message

즉시 확인이 필요합니다.

Weather Flick Batch System
""",
        "level": "error",
        "variables": ["job_id", "job_type", "started_at", "completed_at", "error_message"]
    },
    
    # 슬랙 템플릿
    {
        "event": "job_started",
        "channel": "slack",
        "message_template": """🚀 *배치 작업 시작*
• 작업 ID: `$job_id`
• 작업 유형: `$job_type`
• 시작 시간: $started_at""",
        "level": "info"
    },
    {
        "event": "job_completed",
        "channel": "slack",
        "message_template": """✅ *배치 작업 완료*
• 작업 ID: `$job_id`
• 작업 유형: `$job_type`
• 소요 시간: $started_at ~ $completed_at""",
        "level": "info"
    },
    {
        "event": "job_failed",
        "channel": "slack",
        "message_template": """❌ *배치 작업 실패*
• 작업 ID: `$job_id`
• 작업 유형: `$job_type`
• 에러: $error_message
• 시간: $completed_at""",
        "level": "error"
    },
    {
        "event": "job_retry_started",
        "channel": "slack",
        "message_template": """🔄 *작업 재시도 시작*
• 작업 ID: `$job_id`
• 작업 유형: `$job_type`
• 재시도 횟수: $attempt_number""",
        "level": "warning"
    },
    {
        "event": "job_retry_max_attempts",
        "channel": "slack",
        "message_template": """🚨 *최대 재시도 횟수 도달*
• 작업 ID: `$job_id`
• 작업 유형: `$job_type`
• 시도 횟수: $max_attempts
• 수동 개입이 필요합니다!""",
        "level": "critical"
    }
]


async def setup_templates():
    """템플릿 설정"""
    
    async with aiohttp.ClientSession() as session:
        headers = {
            "X-API-Key": API_KEY,
            "Content-Type": "application/json"
        }
        
        created_count = 0
        failed_count = 0
        
        for template in DEFAULT_TEMPLATES:
            try:
                async with session.post(
                    f"{API_URL}/notifications/templates",
                    headers=headers,
                    json=template
                ) as resp:
                    if resp.status == 200:
                        result = await resp.json()
                        print(f"✅ 템플릿 생성 성공: {template['event']} ({template['channel']})")
                        created_count += 1
                    else:
                        error = await resp.text()
                        if "이미 존재합니다" in error:
                            print(f"⏭️  템플릿 이미 존재: {template['event']} ({template['channel']})")
                        else:
                            print(f"❌ 템플릿 생성 실패: {template['event']} ({template['channel']}) - {error}")
                            failed_count += 1
            except Exception as e:
                print(f"❌ 오류: {template['event']} ({template['channel']}) - {e}")
                failed_count += 1
        
        print(f"\n템플릿 설정 완료:")
        print(f"  - 생성: {created_count}개")
        print(f"  - 실패: {failed_count}개")
        print(f"  - 건너뜀: {len(DEFAULT_TEMPLATES) - created_count - failed_count}개")


if __name__ == "__main__":
    asyncio.run(setup_templates())