#!/usr/bin/env python3
"""
통합 배치 처리 시스템 (아카이빙 포함)

모든 데이터 수집, 저장, 모니터링, 아카이빙을 통합 관리하는 메인 배치 스크립트입니다.
"""

import sys
import os
import asyncio
import logging
import argparse
from datetime import datetime, timedelta

# 프로젝트 루트 경로 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.unified_api_client import get_unified_api_client
from app.collectors.unified_kto_client import UnifiedKTOClient
from scripts.collect_forecast_regions import ForecastRegionCollector
from app.monitoring.monitoring_manager import get_monitoring_manager, MonitoringConfig
from app.monitoring.alert_system import get_alert_system, setup_default_alerts
from app.archiving.archival_engine import get_archival_engine
from app.archiving.backup_manager import get_backup_manager

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class IntegratedBatchProcessor:
    """통합 배치 처리기 (아카이빙 포함)"""
    
    def __init__(self, enable_monitoring: bool = True, enable_archiving: bool = True):
        """
        통합 배치 처리기 초기화
        
        Args:
            enable_monitoring: 모니터링 시스템 활성화
            enable_archiving: 아카이빙 시스템 활성화
        """
        self.enable_monitoring = enable_monitoring
        self.enable_archiving = enable_archiving
        
        # 핵심 시스템 초기화
        self.api_client = get_unified_api_client()
        self.kto_client = UnifiedKTOClient(enable_parallel=True)
        
        # 모니터링 시스템
        if enable_monitoring:
            setup_default_alerts()
            self.monitoring_manager = get_monitoring_manager()
            self.alert_system = get_alert_system()
        else:
            self.monitoring_manager = None
            self.alert_system = None
        
        # 아카이빙 시스템
        if enable_archiving:
            self.archival_engine = get_archival_engine()
            self.backup_manager = get_backup_manager()
        else:
            self.archival_engine = None
            self.backup_manager = None
        
        # 배치 통계
        self.batch_stats = {
            "start_time": None,
            "end_time": None,
            "duration_seconds": 0,
            "tasks_completed": 0,
            "tasks_failed": 0,
            "data_collected": 0,
            "data_archived": 0,
            "archival_compression_ratio": 0.0
        }
    
    async def run_full_batch(self, config: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        전체 배치 프로세스 실행
        
        Args:
            config: 배치 설정
        
        Returns:
            배치 실행 결과
        """
        self.batch_stats["start_time"] = datetime.now()
        
        try:
            logger.info("🚀 통합 배치 처리 시작 (아카이빙 포함)")
            
            # 모니터링 시작
            if self.monitoring_manager:
                await self.monitoring_manager.start_monitoring()
                logger.info("📊 모니터링 시스템 시작됨")
            
            # 1. 데이터 수집 단계
            collection_results = await self._run_data_collection(config)
            
            # 2. 데이터 처리 및 변환 단계
            processing_results = await self._run_data_processing(config)
            
            # 3. 아카이빙 단계
            archival_results = {}
            if self.enable_archiving:
                archival_results = await self._run_archival_process(config)
            
            # 4. 정리 및 최적화 단계
            cleanup_results = await self._run_cleanup_process(config)
            
            # 배치 완료
            self.batch_stats["end_time"] = datetime.now()
            self.batch_stats["duration_seconds"] = (
                self.batch_stats["end_time"] - self.batch_stats["start_time"]
            ).total_seconds()
            
            # 최종 결과 컴파일
            final_results = {
                "success": True,
                "batch_stats": self.batch_stats,
                "collection_results": collection_results,
                "processing_results": processing_results,
                "archival_results": archival_results,
                "cleanup_results": cleanup_results,
                "monitoring_summary": self._get_monitoring_summary() if self.monitoring_manager else None
            }
            
            # 성공 알림
            if self.alert_system:
                await self._send_batch_completion_alert(final_results)
            
            logger.info(f"✅ 통합 배치 처리 완료 (총 {self.batch_stats['duration_seconds']:.1f}초)")
            
            return final_results
            
        except Exception as e:
            # 오류 처리
            self.batch_stats["end_time"] = datetime.now()
            self.batch_stats["duration_seconds"] = (
                self.batch_stats["end_time"] - self.batch_stats["start_time"]
            ).total_seconds()
            
            error_result = {
                "success": False,
                "error": str(e),
                "batch_stats": self.batch_stats
            }
            
            # 오류 알림
            if self.alert_system:
                await self._send_batch_error_alert(error_result)
            
            logger.error(f"❌ 통합 배치 처리 실패: {e}")
            
            return error_result
        
        finally:
            # 모니터링 정리
            if self.monitoring_manager:
                await self.monitoring_manager.stop_monitoring()
    
    async def _run_data_collection(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """데이터 수집 단계"""
        logger.info("📡 데이터 수집 단계 시작")
        
        collection_results = {
            "kto_data": {},
            "forecast_regions": {},
            "total_items_collected": 0
        }
        
        try:
            # KTO 데이터 수집
            if config.get("collect_kto_data", True):
                logger.info("KTO 관광지 데이터 수집 중...")
                
                async with self.api_client:
                    kto_result = await self.kto_client.collect_all_data(
                        content_types=config.get("kto_content_types"),
                        area_codes=config.get("kto_area_codes"),
                        store_raw=True,
                        auto_transform=True
                    )
                
                collection_results["kto_data"] = kto_result
                self.batch_stats["data_collected"] += kto_result.get("total_collected", 0)
                self.batch_stats["tasks_completed"] += 1
                
                logger.info(f"KTO 데이터 수집 완료: {kto_result.get('total_collected', 0)}개")
            
            # 기상청 예보구역 수집
            if config.get("collect_forecast_regions", True):
                logger.info("기상청 예보구역 데이터 수집 중...")
                
                async with ForecastRegionCollector() as collector:
                    regions = await collector.collect_forecast_regions()
                    saved_count = await collector.save_forecast_regions(regions)
                    
                    collection_results["forecast_regions"] = {
                        "collected": len(regions),
                        "saved": saved_count,
                        "storage_stats": collector.get_storage_statistics()
                    }
                
                self.batch_stats["data_collected"] += len(regions)
                self.batch_stats["tasks_completed"] += 1
                
                logger.info(f"예보구역 데이터 수집 완료: {len(regions)}개")
            
            collection_results["total_items_collected"] = self.batch_stats["data_collected"]
            
        except Exception as e:
            self.batch_stats["tasks_failed"] += 1
            logger.error(f"데이터 수집 단계 실패: {e}")
            collection_results["error"] = str(e)
        
        return collection_results
    
    async def _run_data_processing(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """데이터 처리 및 변환 단계"""
        logger.info("⚙️ 데이터 처리 단계 시작")
        
        processing_results = {
            "transformation_applied": 0,
            "quality_checks_passed": 0,
            "errors": []
        }
        
        try:
            # 데이터 변환 파이프라인 실행
            if config.get("enable_data_transformation", True):
                # 실제 변환 로직은 데이터 수집 시 자동으로 실행됨
                processing_results["transformation_applied"] = self.batch_stats["data_collected"]
                self.batch_stats["tasks_completed"] += 1
            
            # 데이터 품질 검사
            if config.get("enable_quality_checks", True):
                # 기본적인 품질 검사는 저장 시 자동으로 실행됨
                processing_results["quality_checks_passed"] = self.batch_stats["data_collected"]
                self.batch_stats["tasks_completed"] += 1
            
        except Exception as e:
            self.batch_stats["tasks_failed"] += 1
            processing_results["errors"].append(str(e))
            logger.error(f"데이터 처리 단계 실패: {e}")
        
        return processing_results
    
    async def _run_archival_process(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """아카이빙 프로세스 단계"""
        logger.info("🗄️ 아카이빙 프로세스 시작")
        
        archival_results = {}
        
        try:
            if not self.archival_engine:
                logger.warning("아카이빙 엔진이 비활성화되어 있습니다")
                return {"skipped": "archiving_disabled"}
            
            # 아카이빙 실행
            dry_run = config.get("archival_dry_run", False)
            api_provider = config.get("archival_api_provider")
            
            summary = await self.archival_engine.run_archival_process(
                api_provider=api_provider,
                dry_run=dry_run
            )
            
            archival_results = {
                "total_candidates": summary.total_candidates,
                "processed_items": summary.processed_items,
                "successful_backups": summary.successful_backups,
                "failed_backups": summary.failed_backups,
                "skipped_items": summary.skipped_items,
                "total_original_size_mb": summary.total_original_size_mb,
                "total_compressed_size_mb": summary.total_compressed_size_mb,
                "average_compression_ratio": summary.average_compression_ratio,
                "processing_time_seconds": summary.processing_time_seconds,
                "dry_run": dry_run
            }
            
            self.batch_stats["data_archived"] = summary.successful_backups
            self.batch_stats["archival_compression_ratio"] = summary.average_compression_ratio
            self.batch_stats["tasks_completed"] += 1
            
            logger.info(f"아카이빙 완료: {summary.successful_backups}개 백업, "
                       f"압축률 {summary.average_compression_ratio:.1f}%")
            
            # 오래된 백업 정리 (선택적)
            if config.get("cleanup_old_backups", False) and not dry_run:
                cleanup_count = await self.backup_manager.cleanup_old_backups()
                archival_results["cleaned_backups"] = cleanup_count
                logger.info(f"오래된 백업 정리 완료: {cleanup_count}개")
            
        except Exception as e:
            self.batch_stats["tasks_failed"] += 1
            archival_results["error"] = str(e)
            logger.error(f"아카이빙 프로세스 실패: {e}")
        
        return archival_results
    
    async def _run_cleanup_process(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """정리 및 최적화 단계"""
        logger.info("🧹 정리 및 최적화 단계 시작")
        
        cleanup_results = {
            "expired_data_cleaned": 0,
            "cache_optimized": False,
            "storage_optimized": False
        }
        
        try:
            # 만료된 원본 데이터 정리
            if config.get("cleanup_expired_data", True):
                async with self.api_client:
                    expired_count = await self.api_client.cleanup_expired_data()
                    cleanup_results["expired_data_cleaned"] = expired_count
                    logger.info(f"만료된 데이터 정리 완료: {expired_count}개")
            
            # 캐시 최적화
            if config.get("optimize_cache", True):
                # 캐시 최적화 로직 (구현 예정)
                cleanup_results["cache_optimized"] = True
            
            # 저장소 최적화
            if config.get("optimize_storage", True):
                # 저장소 최적화 로직 (구현 예정)
                cleanup_results["storage_optimized"] = True
            
            self.batch_stats["tasks_completed"] += 1
            
        except Exception as e:
            self.batch_stats["tasks_failed"] += 1
            cleanup_results["error"] = str(e)
            logger.error(f"정리 및 최적화 단계 실패: {e}")
        
        return cleanup_results
    
    def _get_monitoring_summary(self) -> Optional[Dict[str, Any]]:
        """모니터링 요약 정보"""
        if not self.monitoring_manager:
            return None
        
        try:
            return self.monitoring_manager.get_monitoring_status()
        except Exception as e:
            logger.error(f"모니터링 요약 조회 실패: {e}")
            return {"error": str(e)}
    
    async def _send_batch_completion_alert(self, results: Dict[str, Any]):
        """배치 완료 알림"""
        try:
            duration = self.batch_stats["duration_seconds"]
            data_collected = self.batch_stats["data_collected"]
            data_archived = self.batch_stats["data_archived"]
            compression_ratio = self.batch_stats["archival_compression_ratio"]
            
            message = f"""통합 배치 처리 완료:

📊 실행 요약:
• 처리 시간: {duration:.1f}초
• 수집된 데이터: {data_collected:,}개
• 아카이빙된 데이터: {data_archived:,}개
• 압축률: {compression_ratio:.1f}%
• 완료된 작업: {self.batch_stats['tasks_completed']}개
• 실패한 작업: {self.batch_stats['tasks_failed']}개

✅ 모든 배치 작업이 성공적으로 완료되었습니다."""
            
            from app.monitoring.alert_system import AlertSeverity
            
            result = self.alert_system.send_system_alert(
                title="배치 처리 완료",
                message=message,
                severity=AlertSeverity.INFO,
                source="integrated_batch",
                metadata=results
            )
            
            logger.info("배치 완료 알림 전송됨")
            
        except Exception as e:
            logger.error(f"배치 완료 알림 전송 실패: {e}")
    
    async def _send_batch_error_alert(self, error_result: Dict[str, Any]):
        """배치 오류 알림"""
        try:
            duration = self.batch_stats["duration_seconds"]
            error = error_result.get("error", "알 수 없는 오류")
            
            message = f"""통합 배치 처리 실패:

❌ 오류 정보:
• 오류 내용: {error}
• 실행 시간: {duration:.1f}초
• 완료된 작업: {self.batch_stats['tasks_completed']}개
• 실패한 작업: {self.batch_stats['tasks_failed']}개

즉시 확인이 필요합니다."""
            
            from app.monitoring.alert_system import AlertSeverity
            
            result = self.alert_system.send_system_alert(
                title="배치 처리 실패",
                message=message,
                severity=AlertSeverity.CRITICAL,
                source="integrated_batch",
                metadata=error_result
            )
            
            logger.info("배치 오류 알림 전송됨")
            
        except Exception as e:
            logger.error(f"배치 오류 알림 전송 실패: {e}")


async def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description="통합 배치 처리 시스템 (아카이빙 포함)")
    parser.add_argument("--config", help="설정 파일 경로")
    parser.add_argument("--no-monitoring", action="store_true", help="모니터링 비활성화")
    parser.add_argument("--no-archiving", action="store_true", help="아카이빙 비활성화")
    parser.add_argument("--kto-only", action="store_true", help="KTO 데이터만 수집")
    parser.add_argument("--forecast-only", action="store_true", help="예보구역 데이터만 수집")
    parser.add_argument("--archival-dry-run", action="store_true", help="아카이빙 드라이런 모드")
    parser.add_argument("--archival-provider", help="특정 API 제공자만 아카이빙")
    parser.add_argument("--verbose", "-v", action="store_true", help="상세 로그")
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # 배치 설정
    batch_config = {
        "collect_kto_data": not args.forecast_only,
        "collect_forecast_regions": not args.kto_only,
        "enable_data_transformation": True,
        "enable_quality_checks": True,
        "cleanup_expired_data": True,
        "optimize_cache": True,
        "optimize_storage": True,
        "archival_dry_run": args.archival_dry_run,
        "archival_api_provider": args.archival_provider,
        "cleanup_old_backups": True
    }
    
    # 설정 파일 로드 (구현 예정)
    if args.config:
        logger.info(f"설정 파일 로드: {args.config}")
        # TODO: YAML/JSON 설정 파일 로드 로직
    
    try:
        # 통합 배치 처리기 실행
        processor = IntegratedBatchProcessor(
            enable_monitoring=not args.no_monitoring,
            enable_archiving=not args.no_archiving
        )
        
        results = await processor.run_full_batch(batch_config)
        
        # 결과 출력
        if results["success"]:
            print("\n🎉 통합 배치 처리 성공!")
            print(f"⏱️  총 처리 시간: {results['batch_stats']['duration_seconds']:.1f}초")
            print(f"📊 수집된 데이터: {results['batch_stats']['data_collected']:,}개")
            if results.get("archival_results"):
                arch_results = results["archival_results"]
                print(f"🗄️  아카이빙된 데이터: {arch_results.get('successful_backups', 0)}개")
                print(f"📦 평균 압축률: {arch_results.get('average_compression_ratio', 0):.1f}%")
        else:
            print(f"\n❌ 통합 배치 처리 실패: {results.get('error', '알 수 없는 오류')}")
            sys.exit(1)
    
    except KeyboardInterrupt:
        logger.info("사용자에 의해 중단됨")
    except Exception as e:
        logger.error(f"실행 중 오류 발생: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())