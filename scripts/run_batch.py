#!/usr/bin/env python3
"""
WeatherFlick 배치 작업 수동 실행 도구

개별 배치 작업을 수동으로 실행하거나 전체 상태를 확인할 수 있는 CLI 도구입니다.
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime

# 프로젝트 루트를 sys.path에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from app.schedulers.advanced_scheduler import (
    get_batch_manager,
    BatchJobConfig,
    BatchJobType,
    JobPriority,
    get_all_job_status,
)
from app.core.base_job import JobConfig
from config.constants import JobType
from app.core.logger import get_logger
from config.settings import get_app_settings

# 배치 작업 임포트
from jobs.data_management.weather_update_job import weather_update_task
from jobs.data_management.destination_sync_job import destination_sync_task
from jobs.system_maintenance.log_cleanup_job import log_cleanup_task
from jobs.monitoring.health_check_job import health_check_task
from jobs.recommendation.recommendation_job import RecommendationJob
from jobs.quality.data_quality_job import DataQualityJob

# from jobs.tourism.tourism_sync_job import TourismSyncJob  # 존재하지 않는 모듈
from jobs.tourism.comprehensive_tourism_job import (
    ComprehensiveTourismJob,
    IncrementalTourismJob,
)
from jobs.system_maintenance.database_backup_job import DatabaseBackupJob


class BatchJobRunner:
    """배치 작업 실행기"""

    def __init__(self):
        self.logger = get_logger(__name__)
        self.batch_manager = get_batch_manager()
        self.settings = get_app_settings()

        # 사용 가능한 작업 목록
        self.available_jobs = {
            "weather": {
                "name": "날씨 데이터 업데이트",
                "description": "외부 날씨 API로부터 최신 날씨 정보 수집",
                "function": weather_update_task,
                "job_id": "weather_update",
            },
            "destination": {
                "name": "여행지 정보 동기화",
                "description": "외부 관광 API와 여행지 정보 동기화",
                "function": destination_sync_task,
                "job_id": "destination_sync",
            },
            "tourism": {
                "name": "관광지 데이터 동기화",
                "description": "한국관광공사 API를 통한 관광지 정보 수집",
                "function": self._create_tourism_job,
                "job_id": "tourism_sync",
            },
            "comprehensive-tourism": {
                "name": "종합 관광정보 수집",
                "description": "한국관광공사 API 모든 엔드포인트를 통한 종합적인 관광정보 수집",
                "function": self._create_comprehensive_tourism_job,
                "job_id": "comprehensive_tourism_sync",
            },
            "incremental-tourism": {
                "name": "증분 관광정보 수집",
                "description": "주요 관광정보 일일 증분 업데이트",
                "function": self._create_incremental_tourism_job,
                "job_id": "incremental_tourism_sync",
            },
            "recommendation": {
                "name": "추천 점수 계산",
                "description": "날씨 데이터 기반 지역별 여행 추천 점수 계산",
                "function": self._create_recommendation_job,
                "job_id": "recommendation_update",
            },
            "quality": {
                "name": "데이터 품질 검사",
                "description": "시스템 전체 데이터 품질 검증 및 이상 탐지",
                "function": self._create_quality_job,
                "job_id": "data_quality_check",
            },
            "health": {
                "name": "시스템 헬스체크",
                "description": "시스템 전반의 상태 모니터링 및 이상 감지",
                "function": health_check_task,
                "job_id": "health_check",
            },
            "backup": {
                "name": "데이터베이스 백업",
                "description": "PostgreSQL 데이터베이스 정기 백업",
                "function": self._create_backup_job,
                "job_id": "database_backup",
            },
            "cleanup": {
                "name": "로그 정리",
                "description": "시스템 로그 파일 정리 및 아카이빙",
                "function": log_cleanup_task,
                "job_id": "log_cleanup",
            },
        }

    async def _create_tourism_job(self):
        """관광지 동기화 작업 생성 (ComprehensiveTourismJob 사용)"""
        # TourismSyncJob이 없으므로 ComprehensiveTourismJob으로 대체
        job = ComprehensiveTourismJob()
        return await job.execute()

    def _create_recommendation_job(self):
        """추천 점수 계산 작업 생성"""
        config = BatchJobConfig(
            job_id="recommendation_update",
            job_type=BatchJobType.RECOMMENDATION_UPDATE,
            name="추천 점수 계산",
            description="과거 날씨 데이터를 기반으로 지역별 여행 추천 점수 계산",
            priority=JobPriority.MEDIUM,
            max_instances=1,
            timeout=1800,
            retry_attempts=2,
        )
        job = RecommendationJob(config)
        return job.run()

    def _create_quality_job(self):
        """데이터 품질 검사 작업 생성"""
        config = BatchJobConfig(
            job_id="data_quality_check",
            job_type=BatchJobType.HEALTH_CHECK,
            name="데이터 품질 검사",
            description="시스템 전체 데이터 품질 검증 및 이상 탐지",
            priority=JobPriority.MEDIUM,
            max_instances=1,
            timeout=1200,
            retry_attempts=2,
        )
        job = DataQualityJob(config)
        return job.run()

    async def _create_comprehensive_tourism_job(self):
        """종합 관광정보 수집 작업 생성"""
        job = ComprehensiveTourismJob()
        return await job.execute()

    async def _create_incremental_tourism_job(self):
        """증분 관광정보 수집 작업 생성"""
        job = IncrementalTourismJob()
        return await job.execute()

    def _create_backup_job(self):
        """데이터베이스 백업 작업 생성"""
        config = BatchJobConfig(
            job_id="database_backup",
            job_type=BatchJobType.DATABASE_BACKUP,
            name="데이터베이스 백업",
            description="PostgreSQL 데이터베이스 정기 백업 및 압축 저장",
            priority=JobPriority.HIGH,
            max_instances=1,
            timeout=7200,
            retry_attempts=1,
        )
        job = DatabaseBackupJob(config)
        return job.run()

    def list_jobs(self):
        """사용 가능한 작업 목록 출력"""
        print("\n=== WeatherFlick 배치 작업 목록 ===")
        print(f"{'작업코드':<15} {'작업명':<25} {'설명'}")
        print("-" * 80)

        for job_code, job_info in self.available_jobs.items():
            print(f"{job_code:<15} {job_info['name']:<25} {job_info['description']}")

        print("\n사용법: python run_batch.py run <작업코드>")
        print("예시: python run_batch.py run weather")

    def run_job(self, job_code: str):
        """지정된 작업 실행"""
        if job_code not in self.available_jobs:
            print(f"❌ 알 수 없는 작업 코드: {job_code}")
            print("사용 가능한 작업 목록을 보려면: python run_batch.py list")
            return False

        job_info = self.available_jobs[job_code]

        print(f"\n🚀 배치 작업 시작: {job_info['name']}")
        print(f"📋 설명: {job_info['description']}")
        print(f"⏰ 시작 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("-" * 60)

        try:
            # 작업 함수 실행 (비동기 함수 처리)
            start_time = datetime.now()

            import asyncio
            import inspect

            job_function = job_info["function"]

            # 비동기 함수인지 확인
            if inspect.iscoroutinefunction(job_function):
                result = asyncio.run(job_function())
            else:
                result = job_function()

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            print(f"\n✅ 작업 완료: {job_info['name']}")
            print(f"⏱️  소요 시간: {duration:.2f}초")

            if isinstance(result, dict):
                if "processed_records" in result:
                    print(f"📊 처리 건수: {result['processed_records']}건")
                if "message" in result:
                    print(f"💬 메시지: {result['message']}")
                if "overall_status" in result:
                    status_emoji = {
                        "healthy": "✅",
                        "warning": "⚠️",
                        "critical": "🔥",
                        "unknown": "❓",
                    }
                    status = result["overall_status"]
                    print(
                        f"🏥 시스템 상태: {status_emoji.get(status, '❓')} {status.upper()}"
                    )

            return True

        except Exception as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            print(f"\n❌ 작업 실패: {job_info['name']}")
            print(f"⏱️  소요 시간: {duration:.2f}초")
            print(f"🔥 오류: {str(e)}")

            self.logger.error(f"수동 배치 작업 실패: {job_code}, 오류: {e}")
            return False

    def show_status(self):
        """배치 작업 상태 조회"""
        print("\n=== WeatherFlick 배치 작업 상태 ===")

        try:
            # 스케줄러가 실행 중인지 확인
            if not self.batch_manager.is_running:
                print("⚠️  배치 스케줄러가 실행되지 않음 (수동 실행은 가능)")
            else:
                print("✅ 배치 스케줄러 실행 중")

            # 작업 상태 조회
            job_status = get_all_job_status()

            if not job_status:
                print("📝 등록된 스케줄 작업이 없습니다.")
                return

            print(f"\n{'작업ID':<20} {'작업명':<25} {'다음 실행':<20} {'최근 상태'}")
            print("-" * 85)

            for job_id, status in job_status.items():
                job_name = status.get("name", job_id)
                next_run = status.get("next_run", "N/A")
                last_result = status.get("last_result")

                if next_run and next_run != "N/A":
                    # ISO 형식을 읽기 쉬운 형식으로 변환
                    try:
                        from datetime import datetime

                        dt = datetime.fromisoformat(next_run.replace("Z", "+00:00"))
                        next_run = dt.strftime("%m-%d %H:%M")
                    except:
                        pass

                last_status = "미실행"
                if last_result:
                    if hasattr(last_result, "status"):
                        status_map = {
                            "success": "✅ 성공",
                            "failed": "❌ 실패",
                            "running": "🔄 실행중",
                            "pending": "⏳ 대기중",
                        }
                        last_status = status_map.get(
                            last_result.status.value, last_result.status.value
                        )

                print(f"{job_id:<20} {job_name:<25} {next_run:<20} {last_status}")

        except Exception as e:
            print(f"❌ 상태 조회 실패: {e}")
            self.logger.error(f"배치 상태 조회 실패: {e}")

    def run_all_jobs(self):
        """모든 작업 순차 실행"""
        print("\n🚀 모든 배치 작업 순차 실행 시작")
        print("=" * 60)

        success_count = 0
        total_count = len(self.available_jobs)

        for job_code in self.available_jobs.keys():
            print(f"\n[{success_count + 1}/{total_count}] {job_code} 작업 실행 중...")

            if self.run_job(job_code):
                success_count += 1
            else:
                print(f"⚠️  {job_code} 작업 실패, 다음 작업 계속 진행")

        print(f"\n📊 전체 실행 결과: {success_count}/{total_count} 성공")

        if success_count == total_count:
            print("✅ 모든 작업이 성공적으로 완료되었습니다!")
        else:
            print(f"⚠️  {total_count - success_count}개 작업이 실패했습니다.")


def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(
        description="WeatherFlick 배치 작업 수동 실행 도구",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
사용 예시:
  python run_batch.py list                          # 사용 가능한 작업 목록
  python run_batch.py status                        # 배치 작업 상태 확인
  python run_batch.py run weather                   # 날씨 데이터 업데이트 실행
  python run_batch.py run tourism                   # 관광지 데이터 동기화 실행
  python run_batch.py run comprehensive-tourism     # 종합 관광정보 수집 실행
  python run_batch.py run incremental-tourism       # 증분 관광정보 수집 실행
  python run_batch.py run-all                       # 모든 작업 순차 실행
        """,
    )

    parser.add_argument(
        "command", choices=["list", "run", "status", "run-all"], help="실행할 명령어"
    )

    parser.add_argument(
        "job_code", nargs="?", help="실행할 작업 코드 (run 명령어와 함께 사용)"
    )

    args = parser.parse_args()

    # 로거 초기화
    logger = get_logger(__name__)
    logger.info("WeatherFlick 배치 작업 수동 실행 도구 시작")

    try:
        runner = BatchJobRunner()

        if args.command == "list":
            runner.list_jobs()

        elif args.command == "status":
            runner.show_status()

        elif args.command == "run":
            if not args.job_code:
                print("❌ 작업 코드를 지정해주세요.")
                print("예시: python run_batch.py run weather")
                sys.exit(1)

            success = runner.run_job(args.job_code)
            sys.exit(0 if success else 1)

        elif args.command == "run-all":
            runner.run_all_jobs()

    except KeyboardInterrupt:
        print("\n⚠️  사용자 중단으로 종료합니다.")
        sys.exit(1)
    except Exception as e:
        print(f"❌ 실행 오류: {e}")
        logger.error(f"배치 실행 도구 오류: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
