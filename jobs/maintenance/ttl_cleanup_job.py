#!/usr/bin/env python3
"""
TTL 기반 데이터 정리 작업

설정된 TTL 정책에 따라 주기적으로 만료된 데이터를 정리하는 배치 작업입니다.
"""

import sys
import os
import time
import logging
from datetime import datetime
from typing import Dict, Any

# 프로젝트 루트 경로 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from app.core.ttl_policy_engine import get_ttl_engine
from config.api_storage_policy import GLOBAL_STORAGE_CONFIG

logger = logging.getLogger(__name__)


class TTLCleanupJob:
    """TTL 기반 자동 정리 작업"""
    
    def __init__(self, dry_run: bool = False):
        """
        TTL 정리 작업 초기화
        
        Args:
            dry_run: 실제 삭제하지 않고 시뮬레이션만 수행
        """
        self.dry_run = dry_run
        self.ttl_engine = get_ttl_engine(dry_run=dry_run)
        self.config = GLOBAL_STORAGE_CONFIG
        
        # 작업 통계
        self.job_stats = {
            "runs": 0,
            "total_cleaned": 0,
            "total_space_freed_mb": 0.0,
            "last_run": None,
            "avg_run_time_sec": 0.0,
        }
        
        logger.info(f"TTL 정리 작업 초기화 완료 (dry_run: {dry_run})")
    
    def run_daily_cleanup(self) -> Dict[str, Any]:
        """
        일일 정리 작업 실행
        
        Returns:
            작업 결과 딕셔너리
        """
        logger.info("일일 TTL 정리 작업 시작")
        start_time = time.time()
        
        try:
            # 1. 현재 스토리지 상태 확인
            storage_stats = self.ttl_engine.get_storage_usage_stats()
            logger.info(f"현재 스토리지 상태: "
                       f"{storage_stats.get('overall', {}).get('total_records', 0):,}개 레코드, "
                       f"{storage_stats.get('overall', {}).get('total_size_mb', 0):.2f}MB")
            
            # 2. 정리 대상 식별
            candidates = self.ttl_engine.identify_cleanup_candidates(
                target_space_mb=None,  # 일일 정리는 목표 공간 없이 만료된 데이터 위주
                emergency_mode=False
            )
            
            if not candidates:
                logger.info("정리 대상이 없습니다")
                return self._create_result_summary([], None, time.time() - start_time)
            
            # 3. 정리 실행
            batch_size = self.config.get("auto_cleanup", {}).get("batch_size", 1000)
            cleanup_result = self.ttl_engine.execute_cleanup(candidates, batch_size=batch_size)
            
            # 4. 통계 업데이트
            self._update_job_stats(cleanup_result, time.time() - start_time)
            
            # 5. 결과 요약 생성
            result = self._create_result_summary(candidates, cleanup_result, time.time() - start_time)
            
            logger.info(f"일일 TTL 정리 작업 완료: {cleanup_result.deleted_records}개 삭제, "
                       f"{cleanup_result.space_freed_mb:.2f}MB 확보")
            
            return result
            
        except Exception as e:
            logger.error(f"일일 TTL 정리 작업 오류: {e}")
            return {
                "success": False,
                "error": str(e),
                "execution_time_sec": time.time() - start_time
            }
    
    def run_weekly_cleanup(self) -> Dict[str, Any]:
        """
        주간 정리 작업 실행 (더 적극적인 정리)
        
        Returns:
            작업 결과 딕셔너리
        """
        logger.info("주간 TTL 정리 작업 시작")
        start_time = time.time()
        
        try:
            # 1. 스토리지 상태 확인
            storage_stats = self.ttl_engine.get_storage_usage_stats()
            total_size_mb = storage_stats.get('overall', {}).get('total_size_mb', 0)
            
            # 2. 목표 정리 공간 계산 (전체 크기의 20%)
            target_space_mb = total_size_mb * 0.2
            
            logger.info(f"주간 정리 목표: {target_space_mb:.2f}MB 확보")
            
            # 3. 정리 대상 식별 (더 적극적)
            candidates = self.ttl_engine.identify_cleanup_candidates(
                target_space_mb=target_space_mb,
                emergency_mode=False
            )
            
            if not candidates:
                logger.info("주간 정리 대상이 없습니다")
                return self._create_result_summary([], None, time.time() - start_time)
            
            # 4. 정리 실행
            batch_size = self.config.get("auto_cleanup", {}).get("batch_size", 1000)
            cleanup_result = self.ttl_engine.execute_cleanup(candidates, batch_size=batch_size)
            
            # 5. 통계 업데이트
            self._update_job_stats(cleanup_result, time.time() - start_time)
            
            # 6. 결과 요약 생성
            result = self._create_result_summary(candidates, cleanup_result, time.time() - start_time)
            
            logger.info(f"주간 TTL 정리 작업 완료: {cleanup_result.deleted_records}개 삭제, "
                       f"{cleanup_result.space_freed_mb:.2f}MB 확보")
            
            return result
            
        except Exception as e:
            logger.error(f"주간 TTL 정리 작업 오류: {e}")
            return {
                "success": False,
                "error": str(e),
                "execution_time_sec": time.time() - start_time
            }
    
    def run_emergency_cleanup(self, target_space_gb: float) -> Dict[str, Any]:
        """
        긴급 정리 작업 실행 (디스크 사용률 높을 때)
        
        Args:
            target_space_gb: 목표 확보 공간 (GB)
        
        Returns:
            작업 결과 딕셔너리
        """
        logger.warning(f"긴급 TTL 정리 작업 시작: {target_space_gb}GB 확보 목표")
        start_time = time.time()
        
        try:
            target_space_mb = target_space_gb * 1024
            
            # 1. 긴급 모드로 정리 대상 식별
            candidates = self.ttl_engine.identify_cleanup_candidates(
                target_space_mb=target_space_mb,
                emergency_mode=True
            )
            
            if not candidates:
                logger.warning("긴급 정리 대상이 없습니다")
                return self._create_result_summary([], None, time.time() - start_time)
            
            # 2. 더 큰 배치 크기로 빠른 정리
            batch_size = min(5000, len(candidates))
            cleanup_result = self.ttl_engine.execute_cleanup(candidates, batch_size=batch_size)
            
            # 3. 통계 업데이트
            self._update_job_stats(cleanup_result, time.time() - start_time)
            
            # 4. 결과 요약 생성
            result = self._create_result_summary(candidates, cleanup_result, time.time() - start_time)
            result["emergency_mode"] = True
            
            logger.warning(f"긴급 TTL 정리 작업 완료: {cleanup_result.deleted_records}개 삭제, "
                          f"{cleanup_result.space_freed_mb:.2f}MB 확보")
            
            return result
            
        except Exception as e:
            logger.error(f"긴급 TTL 정리 작업 오류: {e}")
            return {
                "success": False,
                "error": str(e),
                "emergency_mode": True,
                "execution_time_sec": time.time() - start_time
            }
    
    def check_cleanup_needed(self) -> Dict[str, Any]:
        """
        정리 작업 필요성 확인
        
        Returns:
            정리 필요성 분석 결과
        """
        try:
            storage_stats = self.ttl_engine.get_storage_usage_stats()
            overall_stats = storage_stats.get('overall', {})
            
            total_records = overall_stats.get('total_records', 0)
            total_size_mb = overall_stats.get('total_size_mb', 0)
            old_records_90d = storage_stats.get('cleanup_potential', {}).get('old_records_90d', 0)
            large_records_10mb = storage_stats.get('cleanup_potential', {}).get('large_records_10mb', 0)
            
            # 정리 필요성 평가
            cleanup_score = 0
            recommendations = []
            
            # 1. 오래된 데이터 비율
            if total_records > 0:
                old_ratio = old_records_90d / total_records
                if old_ratio > 0.3:  # 30% 이상이 90일 이상
                    cleanup_score += 3
                    recommendations.append("90일 이상된 데이터가 30% 이상 - 일일 정리 권장")
                elif old_ratio > 0.1:  # 10% 이상이 90일 이상
                    cleanup_score += 1
                    recommendations.append("90일 이상된 데이터 정리 검토 필요")
            
            # 2. 대용량 파일 비율
            if total_records > 0:
                large_ratio = large_records_10mb / total_records
                if large_ratio > 0.1:  # 10% 이상이 10MB 이상
                    cleanup_score += 2
                    recommendations.append("대용량 파일(10MB+) 정리 검토 필요")
            
            # 3. 전체 크기
            if total_size_mb > 1000:  # 1GB 이상
                cleanup_score += 1
                recommendations.append("전체 저장 공간이 1GB 이상 - 주간 정리 권장")
            
            # 4. 레코드 수
            if total_records > 50000:  # 5만개 이상
                cleanup_score += 1
                recommendations.append("레코드 수가 5만개 이상 - 정기 정리 필요")
            
            # 정리 우선순위 결정
            if cleanup_score >= 4:
                priority = "high"
                action = "즉시 정리 작업 실행 권장"
            elif cleanup_score >= 2:
                priority = "medium"
                action = "일주일 내 정리 작업 실행 권장"
            elif cleanup_score >= 1:
                priority = "low"
                action = "정기 정리 작업으로 충분"
            else:
                priority = "none"
                action = "정리 작업 불필요"
            
            return {
                "cleanup_needed": cleanup_score > 0,
                "cleanup_score": cleanup_score,
                "priority": priority,
                "action": action,
                "recommendations": recommendations,
                "storage_stats": storage_stats,
                "analysis_time": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"정리 필요성 확인 오류: {e}")
            return {
                "cleanup_needed": False,
                "error": str(e)
            }
    
    def _update_job_stats(self, cleanup_result, execution_time_sec: float):
        """작업 통계 업데이트"""
        self.job_stats["runs"] += 1
        self.job_stats["total_cleaned"] += cleanup_result.deleted_records
        self.job_stats["total_space_freed_mb"] += cleanup_result.space_freed_mb
        self.job_stats["last_run"] = datetime.now()
        self.job_stats["avg_run_time_sec"] = (
            (self.job_stats["avg_run_time_sec"] + execution_time_sec) / 2
        )
    
    def _create_result_summary(self, candidates, cleanup_result, execution_time_sec: float) -> Dict[str, Any]:
        """작업 결과 요약 생성"""
        if cleanup_result is None:
            return {
                "success": True,
                "candidates_found": len(candidates),
                "deleted_records": 0,
                "space_freed_mb": 0.0,
                "execution_time_sec": execution_time_sec,
                "dry_run": self.dry_run
            }
        
        return {
            "success": len(cleanup_result.errors) == 0,
            "candidates_found": len(candidates),
            "deleted_records": cleanup_result.deleted_records,
            "space_freed_mb": cleanup_result.space_freed_mb,
            "execution_time_sec": execution_time_sec,
            "errors": cleanup_result.errors,
            "cleanup_summary": cleanup_result.cleanup_summary,
            "dry_run": self.dry_run
        }
    
    def get_job_statistics(self) -> Dict[str, Any]:
        """작업 통계 반환"""
        return {
            **self.job_stats,
            "ttl_engine_stats": self.ttl_engine.get_statistics()
        }


def main():
    """메인 실행 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description='TTL 기반 데이터 정리 작업')
    parser.add_argument('--mode', choices=['daily', 'weekly', 'emergency', 'check'], 
                       default='daily', help='정리 모드')
    parser.add_argument('--dry-run', action='store_true', 
                       help='실제 삭제하지 않고 시뮬레이션만 수행')
    parser.add_argument('--target-space-gb', type=float, default=1.0,
                       help='긴급 모드 시 목표 확보 공간 (GB)')
    parser.add_argument('--verbose', '-v', action='store_true', help='상세 로그 출력')
    
    args = parser.parse_args()
    
    # 로깅 설정
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        print(f"🧹 TTL 기반 데이터 정리 작업 시작 ({args.mode} 모드)")
        print(f"   Dry-run: {'ON' if args.dry_run else 'OFF'}")
        
        cleanup_job = TTLCleanupJob(dry_run=args.dry_run)
        
        if args.mode == 'daily':
            result = cleanup_job.run_daily_cleanup()
        elif args.mode == 'weekly':
            result = cleanup_job.run_weekly_cleanup()
        elif args.mode == 'emergency':
            result = cleanup_job.run_emergency_cleanup(args.target_space_gb)
        elif args.mode == 'check':
            result = cleanup_job.check_cleanup_needed()
        
        # 결과 출력
        print("\n" + "="*60)
        print("📊 TTL 정리 작업 결과")
        print("="*60)
        
        if args.mode == 'check':
            print(f"정리 필요성: {'예' if result.get('cleanup_needed', False) else '아니오'}")
            print(f"우선순위: {result.get('priority', 'N/A')}")
            print(f"권장 조치: {result.get('action', 'N/A')}")
            if result.get('recommendations'):
                print("권장사항:")
                for rec in result['recommendations']:
                    print(f"  • {rec}")
        else:
            success = result.get('success', False)
            print(f"작업 성공: {'✅' if success else '❌'}")
            print(f"정리 대상: {result.get('candidates_found', 0):,}개")
            print(f"삭제된 레코드: {result.get('deleted_records', 0):,}개")
            print(f"확보된 공간: {result.get('space_freed_mb', 0):.2f}MB")
            print(f"실행 시간: {result.get('execution_time_sec', 0):.2f}초")
            
            if result.get('errors'):
                print(f"오류: {len(result['errors'])}개")
                for error in result['errors'][:3]:  # 최대 3개만 표시
                    print(f"  • {error}")
        
        print("\n✅ TTL 정리 작업 완료")
        
    except Exception as e:
        print(f"❌ TTL 정리 작업 실패: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()