#!/usr/bin/env python3
"""
아카이빙 프로세스 실행 스크립트

API 원본 데이터의 아카이빙을 수동으로 실행하거나 스케줄링하기 위한 스크립트입니다.
"""

import sys
import os
import asyncio
import logging
import argparse

# 프로젝트 루트 경로 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.archiving.archival_engine import get_archival_engine
from app.archiving.backup_manager import get_backup_manager, BackupConfiguration
from app.archiving.archival_policies import get_archival_policy_manager

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def run_archival_process(api_provider: str = None, endpoint: str = None,
                             dry_run: bool = False, cleanup_old_backups: bool = False):
    """아카이빙 프로세스 실행"""
    logger.info("🗄️ 아카이빙 프로세스 시작")

    try:
        # 아카이빙 엔진 초기화
        backup_config = BackupConfiguration(
            base_backup_path="./data/backups",
            max_concurrent_backups=3,
            verify_integrity=True,
            auto_cleanup_days=30
        )
        backup_manager = get_backup_manager(backup_config)
        archival_engine = get_archival_engine(backup_manager)

        # 정책 상태 확인
        policy_manager = get_archival_policy_manager()
        policy_stats = policy_manager.get_policy_statistics()
        logger.info(f"📋 아카이빙 정책 상태: {policy_stats['enabled_policies']}개 활성화됨")

        # 아카이빙 실행
        summary = await archival_engine.run_archival_process(
            api_provider=api_provider,
            endpoint=endpoint,
            dry_run=dry_run
        )

        # 결과 리포트
        logger.info("📊 아카이빙 결과 요약:")
        logger.info(f"  • 총 후보: {summary.total_candidates}개")
        logger.info(f"  • 처리된 항목: {summary.processed_items}개")
        logger.info(f"  • 성공한 백업: {summary.successful_backups}개")
        logger.info(f"  • 실패한 백업: {summary.failed_backups}개")
        logger.info(f"  • 건너뛴 항목: {summary.skipped_items}개")
        logger.info(f"  • 원본 크기: {summary.total_original_size_mb:.2f} MB")
        logger.info(f"  • 압축 크기: {summary.total_compressed_size_mb:.2f} MB")
        logger.info(f"  • 평균 압축률: {summary.average_compression_ratio:.1f}%")
        logger.info(f"  • 처리 시간: {summary.processing_time_seconds:.2f}초")

        # 오래된 백업 정리 (옵션)
        if cleanup_old_backups and not dry_run:
            logger.info("🧹 오래된 백업 정리 시작")
            cleaned_count = await backup_manager.cleanup_old_backups()
            logger.info(f"✅ 오래된 백업 정리 완료: {cleaned_count}개 삭제")

        # 전체 통계
        archival_stats = archival_engine.get_archival_statistics()
        logger.info("📈 아카이빙 엔진 통계:")
        logger.info(f"  • 총 실행 횟수: {archival_stats['engine_statistics']['total_runs']}")
        logger.info(f"  • 총 처리 항목: {archival_stats['engine_statistics']['total_items_processed']}")
        logger.info(f"  • 총 백업 생성: {archival_stats['engine_statistics']['total_backups_created']}")
        logger.info(f"  • 총 아카이빙 데이터: {archival_stats['engine_statistics']['total_data_archived_mb']:.2f} MB")

        logger.info("✅ 아카이빙 프로세스 완료")

    except Exception as e:
        logger.error(f"❌ 아카이빙 프로세스 실패: {e}")
        raise


async def show_policy_information():
    """아카이빙 정책 정보 표시"""
    logger.info("📋 아카이빙 정책 정보")

    policy_manager = get_archival_policy_manager()
    policies = policy_manager.get_all_policies()

    for policy_id, policy in policies.items():
        logger.info(f"\n정책: {policy.name}")
        logger.info(f"  • ID: {policy_id}")
        logger.info(f"  • 제공자: {policy.api_provider}")
        logger.info(f"  • 활성화: {policy.enabled}")
        logger.info(f"  • 규칙 수: {len(policy.rules)}")

        for rule in policy.rules:
            logger.info(f"    - {rule.name}")
            logger.info(f"      트리거: {rule.trigger.value}")
            logger.info(f"      조건: {rule.condition}")
            logger.info(f"      압축: {rule.compression.value}")
            logger.info(f"      저장 위치: {rule.target_location.value}")
            logger.info(f"      보존 기간: {rule.retention_days}일")


async def show_backup_statistics():
    """백업 통계 표시"""
    logger.info("📊 백업 통계")

    backup_manager = get_backup_manager()
    stats = backup_manager.get_backup_statistics()

    logger.info(f"총 백업 수: {stats['total_backups']}")
    logger.info(f"성공한 백업: {stats['successful_backups']}")
    logger.info(f"실패한 백업: {stats['failed_backups']}")
    logger.info(f"원본 총 크기: {stats['total_original_size_bytes'] / (1024*1024):.2f} MB")
    logger.info(f"압축 총 크기: {stats['total_compressed_size_bytes'] / (1024*1024):.2f} MB")
    logger.info(f"평균 압축률: {stats['average_compression_ratio']:.1f}%")

    logger.info("\n상태별 백업 수:")
    for status, count in stats['backup_by_status'].items():
        logger.info(f"  • {status}: {count}개")

    logger.info("\n제공자별 백업 수:")
    for provider, count in stats['backup_by_provider'].items():
        logger.info(f"  • {provider}: {count}개")


async def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description="아카이빙 프로세스 실행")
    parser.add_argument("--provider", help="특정 API 제공자만 처리 (예: KTO, KMA)")
    parser.add_argument("--endpoint", help="특정 엔드포인트만 처리")
    parser.add_argument("--dry-run", action="store_true", help="실제 실행 없이 분석만 수행")
    parser.add_argument("--cleanup", action="store_true", help="오래된 백업 정리")
    parser.add_argument("--show-policies", action="store_true", help="아카이빙 정책 정보 표시")
    parser.add_argument("--show-stats", action="store_true", help="백업 통계 표시")

    args = parser.parse_args()

    try:
        if args.show_policies:
            await show_policy_information()
        elif args.show_stats:
            await show_backup_statistics()
        else:
            await run_archival_process(
                api_provider=args.provider,
                endpoint=args.endpoint,
                dry_run=args.dry_run,
                cleanup_old_backups=args.cleanup
            )

    except KeyboardInterrupt:
        logger.info("사용자에 의해 중단됨")
    except Exception as e:
        logger.error(f"실행 중 오류 발생: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())