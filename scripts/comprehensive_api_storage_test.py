#!/usr/bin/env python3
"""
API 저장 정책 및 아카이빙 시스템 종합 테스트

1. API 원본 데이터 저장 테스트
2. 데이터 만료 정책 테스트
3. 자동 아카이빙 기능 테스트
4. 저장 공간 최적화 테스트
5. 백업 및 복구 시스템 테스트
6. 데이터 압축 및 보관 정책 테스트
"""

import sys
import os
import asyncio
import logging
import tempfile
import shutil
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any

# 프로젝트 루트 경로 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.api_storage_policy_engine import APIStoragePolicyEngine, get_policy_engine
from app.core.database_manager import SyncDatabaseManager
from app.archiving.backup_manager import BackupManager, BackupConfiguration
from app.archiving.archival_engine import ArchivalEngine
from app.archiving.archival_policies import get_archival_policy_manager
from config.api_storage_policy import ProviderConfig, EndpointConfig, StoragePolicy

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ComprehensiveAPIStorageTest:
    """API 저장 시스템 종합 테스트 클래스"""
    
    def __init__(self):
        """테스트 초기화"""
        self.start_time = datetime.now()
        self.test_results = {}
        self.temp_backup_dir = None
        self.db_manager = SyncDatabaseManager()
        self.policy_engine = get_policy_engine()
        
        # 테스트 통계
        self.stats = {
            "total_tests": 0,
            "passed_tests": 0,
            "failed_tests": 0,
            "errors": []
        }
    
    def run_all_tests(self):
        """모든 테스트 실행"""
        logger.info("🚀 API 저장 정책 및 아카이빙 시스템 종합 테스트 시작")
        
        try:
            # 1. API 원본 데이터 저장 테스트
            self.test_api_storage_policy()
            
            # 2. 데이터 만료 정책 테스트
            self.test_ttl_policy()
            
            # 3. 자동 아카이빙 기능 테스트
            asyncio.run(self.test_archival_system())
            
            # 4. 저장 공간 최적화 테스트
            self.test_storage_optimization()
            
            # 5. 백업 및 복구 시스템 테스트
            asyncio.run(self.test_backup_recovery())
            
            # 6. 데이터 압축 및 보관 정책 테스트
            self.test_compression_policy()
            
            # 결과 출력
            self.generate_final_report()
            
        except Exception as e:
            logger.error(f"종합 테스트 실행 중 오류: {e}")
            self.stats["errors"].append(str(e))
        
        finally:
            self.cleanup()
    
    def test_api_storage_policy(self):
        """API 저장 정책 테스트"""
        logger.info("📋 1. API 원본 데이터 저장 정책 테스트")
        
        test_cases = [
            # KMA 테스트 케이스
            {
                "name": "KMA 예보구역 API - 정상 응답",
                "provider": "KMA",
                "endpoint": "fct_shrt_reg",
                "response_size": 500_000,  # 500KB
                "status_code": 200,
                "expected": True
            },
            {
                "name": "KMA 예보구역 API - 크기 초과",
                "provider": "KMA",
                "endpoint": "fct_shrt_reg",
                "response_size": 2_000_000,  # 2MB (제한: 1MB)
                "status_code": 200,
                "expected": False
            },
            {
                "name": "KMA 헬스체크 - 저장 비활성화",
                "provider": "KMA",
                "endpoint": "health",
                "response_size": 100,
                "status_code": 200,
                "expected": False
            },
            {
                "name": "KMA 오류 응답 - 저장 활성화",
                "provider": "KMA",
                "endpoint": "fct_shrt_reg",
                "response_size": 1000,
                "status_code": 500,
                "expected": True
            },
            
            # KTO 테스트 케이스
            {
                "name": "KTO 지역코드 API - 정상 응답",
                "provider": "KTO",
                "endpoint": "areaCode2",
                "response_size": 500_000,
                "status_code": 200,
                "expected": True
            },
            {
                "name": "KTO 이미지 API - 대용량",
                "provider": "KTO",
                "endpoint": "detailImage2",
                "response_size": 25_000_000,  # 25MB
                "status_code": 200,
                "expected": True  # KTO는 30MB까지 허용
            },
            {
                "name": "KTO 이미지 API - 크기 초과",
                "provider": "KTO",
                "endpoint": "detailImage2",
                "response_size": 35_000_000,  # 35MB (제한: 30MB)
                "status_code": 200,
                "expected": False
            },
            
            # 미지원 제공자
            {
                "name": "알 수 없는 제공자",
                "provider": "UNKNOWN",
                "endpoint": "test",
                "response_size": 1000,
                "status_code": 200,
                "expected": False
            },
            
            # 모니터링 제공자 (비활성화)
            {
                "name": "모니터링 API - 비활성화",
                "provider": "MONITORING",
                "endpoint": "health",
                "response_size": 100,
                "status_code": 200,
                "expected": False
            }
        ]
        
        passed_cases = 0
        total_cases = len(test_cases)
        
        for test_case in test_cases:
            self.stats["total_tests"] += 1
            
            try:
                should_store, reason = self.policy_engine.should_store(
                    provider=test_case["provider"],
                    endpoint=test_case["endpoint"],
                    response_size_bytes=test_case["response_size"],
                    status_code=test_case["status_code"]
                )
                
                if should_store == test_case["expected"]:
                    logger.info(f"✅ {test_case['name']}: PASS - {reason}")
                    passed_cases += 1
                    self.stats["passed_tests"] += 1
                else:
                    logger.error(f"❌ {test_case['name']}: FAIL - 예상: {test_case['expected']}, 실제: {should_store}")
                    self.stats["failed_tests"] += 1
                    self.stats["errors"].append(f"저장 정책 테스트 실패: {test_case['name']}")
                    
            except Exception as e:
                logger.error(f"❌ {test_case['name']}: ERROR - {e}")
                self.stats["failed_tests"] += 1
                self.stats["errors"].append(f"저장 정책 테스트 오류: {test_case['name']} - {e}")
        
        # 메타데이터 생성 테스트
        self.stats["total_tests"] += 1
        try:
            metadata = self.policy_engine.get_storage_metadata("KMA", "fct_shrt_reg")
            if all(key in metadata for key in ["ttl_days", "priority", "expires_at", "compression_enabled"]):
                logger.info("✅ 저장 메타데이터 생성: PASS")
                passed_cases += 1
                self.stats["passed_tests"] += 1
            else:
                logger.error("❌ 저장 메타데이터 생성: FAIL - 필수 키 누락")
                self.stats["failed_tests"] += 1
        except Exception as e:
            logger.error(f"❌ 저장 메타데이터 생성: ERROR - {e}")
            self.stats["failed_tests"] += 1
        
        # 통계 확인
        stats = self.policy_engine.get_statistics()
        logger.info(f"📊 정책 엔진 통계: {stats}")
        
        self.test_results["api_storage_policy"] = {
            "passed": passed_cases,
            "total": total_cases + 1,
            "success_rate": round(passed_cases / (total_cases + 1) * 100, 2)
        }
        
        logger.info(f"📋 API 저장 정책 테스트 완료: {passed_cases}/{total_cases + 1} 통과")
    
    def test_ttl_policy(self):
        """TTL 정책 테스트"""
        logger.info("⏰ 2. 데이터 만료 정책 테스트")
        
        # 실제 데이터베이스의 만료 대상 데이터 확인
        try:
            # 만료된 데이터 조회 (단순 쿼리)
            expired_query = """
                SELECT COUNT(*) as count, 
                       COUNT(CASE WHEN expires_at < NOW() THEN 1 END) as expired_count,
                       COUNT(CASE WHEN created_at < NOW() - INTERVAL '90 days' THEN 1 END) as old_count
                FROM api_raw_data
            """
            
            result = self.db_manager.fetch_one(expired_query)
            
            logger.info(f"📊 데이터베이스 만료 분석:")
            logger.info(f"  - 전체 레코드: {result['count']:,}개")
            logger.info(f"  - 만료된 레코드: {result['expired_count']:,}개")
            logger.info(f"  - 90일 이상 된 레코드: {result['old_count']:,}개")
            
            # 제공자별 분석
            provider_query = """
                SELECT api_provider, 
                       COUNT(*) as total,
                       COUNT(CASE WHEN expires_at < NOW() THEN 1 END) as expired,
                       AVG(response_size) as avg_size_bytes
                FROM api_raw_data
                GROUP BY api_provider
                ORDER BY total DESC
            """
            
            providers = self.db_manager.fetch_all(provider_query)
            
            logger.info("📊 제공자별 데이터 분석:")
            for provider in providers:
                avg_size_mb = (provider['avg_size_bytes'] or 0) / (1024 * 1024)
                logger.info(f"  - {provider['api_provider']}: {provider['total']:,}개 (만료: {provider['expired']:,}개, 평균크기: {avg_size_mb:.2f}MB)")
            
            # 크기별 분석
            size_query = """
                SELECT 
                    COUNT(CASE WHEN response_size > 10*1024*1024 THEN 1 END) as large_files,
                    COUNT(CASE WHEN response_size > 1*1024*1024 THEN 1 END) as medium_files,
                    COUNT(CASE WHEN response_size <= 1*1024*1024 THEN 1 END) as small_files,
                    SUM(response_size)::bigint as total_size_bytes
                FROM api_raw_data
            """
            
            size_result = self.db_manager.fetch_one(size_query)
            total_size_mb = (size_result['total_size_bytes'] or 0) / (1024 * 1024)
            
            logger.info(f"📊 크기별 데이터 분석:")
            logger.info(f"  - 대용량 파일 (>10MB): {size_result['large_files']:,}개")
            logger.info(f"  - 중간 파일 (1-10MB): {size_result['medium_files']:,}개")
            logger.info(f"  - 소용량 파일 (<1MB): {size_result['small_files']:,}개")
            logger.info(f"  - 전체 데이터 크기: {total_size_mb:.2f}MB")
            
            self.stats["total_tests"] += 1
            self.stats["passed_tests"] += 1
            
            self.test_results["ttl_policy"] = {
                "total_records": result['count'],
                "expired_records": result['expired_count'],
                "old_records": result['old_count'],
                "total_size_mb": round(total_size_mb, 2),
                "providers": len(providers)
            }
            
            logger.info("✅ TTL 정책 분석 완료")
            
        except Exception as e:
            logger.error(f"❌ TTL 정책 테스트 실패: {e}")
            self.stats["total_tests"] += 1
            self.stats["failed_tests"] += 1
            self.stats["errors"].append(f"TTL 정책 테스트 오류: {e}")
    
    async def test_archival_system(self):
        """아카이빙 시스템 테스트"""
        logger.info("🗄️ 3. 자동 아카이빙 기능 테스트")
        
        # 임시 백업 디렉토리 생성
        self.temp_backup_dir = tempfile.mkdtemp(prefix="archival_test_")
        logger.info(f"임시 백업 디렉토리: {self.temp_backup_dir}")
        
        try:
            # 백업 설정
            config = BackupConfiguration(
                base_backup_path=self.temp_backup_dir,
                max_concurrent_backups=3,
                verify_integrity=True
            )
            
            backup_manager = BackupManager(config)
            archival_engine = ArchivalEngine(backup_manager)
            
            # 가상 아카이빙 데이터 생성 및 테스트
            test_data = {
                "api_response": {
                    "resultCode": "0000",
                    "resultMsg": "OK",
                    "items": [
                        {"contentid": "123", "title": "테스트 관광지 1"},
                        {"contentid": "124", "title": "테스트 관광지 2"}
                    ]
                },
                "metadata": {
                    "query_time": datetime.now().isoformat(),
                    "total_count": 2
                }
            }
            
            # 아카이빙 정책 매니저 테스트
            policy_manager = get_archival_policy_manager()
            policies = policy_manager.get_all_policies()
            
            logger.info(f"📋 아카이빙 정책: {len(policies)}개 로드됨")
            
            # 백업 생성 테스트
            from app.archiving.archival_policies import ArchivalRule, ArchivalTrigger, CompressionType, StorageLocation
            
            test_rule = ArchivalRule(
                rule_id="test_rule",
                name="테스트 백업 규칙",
                description="테스트용 백업 규칙",
                trigger=ArchivalTrigger.MANUAL,
                condition={},
                target_location=StorageLocation.LOCAL_DISK,
                compression=CompressionType.GZIP,
                retention_days=30
            )
            
            backup_record = await backup_manager.backup_data(
                data_id="test_123",
                api_provider="TEST",
                endpoint="test_endpoint",
                data=test_data,
                rule=test_rule
            )
            
            if backup_record and backup_record.status.value == "completed":
                logger.info(f"✅ 백업 생성 성공: {backup_record.backup_id}")
                logger.info(f"   압축률: {backup_record.compression_ratio:.1f}%")
                
                # 복원 테스트
                restored_data = await backup_manager.restore_data(backup_record.backup_id)
                
                if restored_data == test_data:
                    logger.info("✅ 백업 복원 성공: 데이터 일치 확인")
                    self.stats["passed_tests"] += 2
                else:
                    logger.error("❌ 백업 복원 실패: 데이터 불일치")
                    self.stats["failed_tests"] += 1
                    
                self.stats["total_tests"] += 2
                
            else:
                logger.error("❌ 백업 생성 실패")
                self.stats["total_tests"] += 1
                self.stats["failed_tests"] += 1
            
            # 아카이빙 엔진 통계
            stats = archival_engine.get_archival_statistics()
            logger.info(f"📊 아카이빙 엔진 통계: {stats['engine_statistics']}")
            
            self.test_results["archival_system"] = {
                "backup_created": backup_record is not None,
                "compression_ratio": backup_record.compression_ratio if backup_record else 0,
                "policies_count": len(policies)
            }
            
            logger.info("✅ 아카이빙 시스템 테스트 완료")
            
        except Exception as e:
            logger.error(f"❌ 아카이빙 시스템 테스트 실패: {e}")
            self.stats["total_tests"] += 1
            self.stats["failed_tests"] += 1
            self.stats["errors"].append(f"아카이빙 시스템 오류: {e}")
    
    def test_storage_optimization(self):
        """저장 공간 최적화 테스트"""
        logger.info("💾 4. 저장 공간 최적화 테스트")
        
        try:
            # 데이터베이스 디스크 사용량 분석
            disk_usage_query = """
                SELECT 
                    schemaname,
                    tablename,
                    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
                    pg_total_relation_size(schemaname||'.'||tablename) as size_bytes
                FROM pg_tables 
                WHERE schemaname = 'public'
                ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
                LIMIT 10
            """
            
            tables = self.db_manager.fetch_all(disk_usage_query)
            
            logger.info("📊 테이블별 디스크 사용량 (상위 10개):")
            total_size_bytes = 0
            for table in tables:
                size_mb = table['size_bytes'] / (1024 * 1024)
                total_size_bytes += table['size_bytes']
                logger.info(f"  - {table['tablename']}: {table['size']} ({size_mb:.1f}MB)")
            
            total_size_gb = total_size_bytes / (1024 * 1024 * 1024)
            logger.info(f"📊 전체 데이터베이스 크기: {total_size_gb:.2f}GB")
            
            # API 원본 데이터 최적화 기회 분석
            optimization_query = """
                SELECT 
                    api_provider,
                    endpoint,
                    COUNT(*) as record_count,
                    SUM(response_size) as total_size_bytes,
                    AVG(response_size) as avg_size_bytes,
                    COUNT(CASE WHEN expires_at < NOW() THEN 1 END) as expired_count,
                    COUNT(CASE WHEN response_size > 5*1024*1024 THEN 1 END) as large_files_count
                FROM api_raw_data
                GROUP BY api_provider, endpoint
                HAVING COUNT(*) > 10
                ORDER BY SUM(response_size) DESC
                LIMIT 20
            """
            
            optimizations = self.db_manager.fetch_all(optimization_query)
            
            logger.info("📊 API 데이터 최적화 분석 (상위 20개):")
            total_optimizable_mb = 0
            
            for opt in optimizations:
                size_mb = (opt['total_size_bytes'] or 0) / (1024 * 1024)
                avg_size_kb = (opt['avg_size_bytes'] or 0) / 1024
                total_optimizable_mb += size_mb
                
                # 최적화 기회 평가
                optimization_potential = ""
                if opt['expired_count'] > 0:
                    optimization_potential += f"만료:{opt['expired_count']}개 "
                if opt['large_files_count'] > 0:
                    optimization_potential += f"대용량:{opt['large_files_count']}개 "
                
                logger.info(f"  - {opt['api_provider']}/{opt['endpoint']}: {size_mb:.1f}MB, {opt['record_count']}개 (평균: {avg_size_kb:.1f}KB) {optimization_potential}")
            
            # 압축 효율성 테스트
            compression_test_data = json.dumps({
                "test_data": ["sample"] * 1000,
                "metadata": {"created": datetime.now().isoformat()}
            }, ensure_ascii=False)
            
            original_size = len(compression_test_data.encode('utf-8'))
            
            import gzip
            compressed_data = gzip.compress(compression_test_data.encode('utf-8'))
            compressed_size = len(compressed_data)
            compression_ratio = (1 - compressed_size / original_size) * 100
            
            logger.info(f"📊 압축 효율성 테스트:")
            logger.info(f"  - 원본 크기: {original_size:,} bytes")
            logger.info(f"  - 압축 크기: {compressed_size:,} bytes")
            logger.info(f"  - 압축률: {compression_ratio:.1f}%")
            
            self.test_results["storage_optimization"] = {
                "total_db_size_gb": round(total_size_gb, 2),
                "optimizable_data_mb": round(total_optimizable_mb, 2),
                "compression_ratio": round(compression_ratio, 1),
                "optimization_candidates": len(optimizations)
            }
            
            self.stats["total_tests"] += 1
            self.stats["passed_tests"] += 1
            
            logger.info("✅ 저장 공간 최적화 분석 완료")
            
        except Exception as e:
            logger.error(f"❌ 저장 공간 최적화 테스트 실패: {e}")
            self.stats["total_tests"] += 1
            self.stats["failed_tests"] += 1
            self.stats["errors"].append(f"저장 공간 최적화 오류: {e}")
    
    async def test_backup_recovery(self):
        """백업 및 복구 시스템 테스트"""
        logger.info("💿 5. 백업 및 복구 시스템 테스트")
        
        if not self.temp_backup_dir:
            self.temp_backup_dir = tempfile.mkdtemp(prefix="backup_test_")
        
        try:
            config = BackupConfiguration(
                base_backup_path=self.temp_backup_dir,
                max_concurrent_backups=2,
                verify_integrity=True
            )
            
            backup_manager = BackupManager(config)
            
            # 다양한 크기의 테스트 데이터 생성
            test_datasets = [
                {
                    "name": "small_dataset",
                    "data": {"items": [f"item_{i}" for i in range(10)]},
                    "expected_compression": 50  # 예상 압축률 (%)
                },
                {
                    "name": "medium_dataset", 
                    "data": {"items": [{"id": i, "description": "테스트 데이터 " * 10} for i in range(100)]},
                    "expected_compression": 80
                },
                {
                    "name": "large_dataset",
                    "data": {"items": [{"id": i, "content": "대용량 테스트 데이터 " * 100} for i in range(1000)]},
                    "expected_compression": 90
                }
            ]
            
            backup_results = []
            
            for dataset in test_datasets:
                # 백업 규칙 생성
                from app.archiving.archival_policies import ArchivalRule, ArchivalTrigger, CompressionType, StorageLocation
                
                rule = ArchivalRule(
                    rule_id=f"test_rule_{dataset['name']}",
                    name=f"테스트 규칙 - {dataset['name']}",
                    description=f"{dataset['name']} 백업 테스트",
                    trigger=ArchivalTrigger.MANUAL,
                    condition={},
                    target_location=StorageLocation.LOCAL_DISK,
                    compression=CompressionType.GZIP,
                    retention_days=7
                )
                
                # 백업 실행
                start_time = time.time()
                backup_record = await backup_manager.backup_data(
                    data_id=f"test_{dataset['name']}",
                    api_provider="TEST",
                    endpoint="backup_test",
                    data=dataset["data"],
                    rule=rule
                )
                backup_time = time.time() - start_time
                
                if backup_record:
                    # 복원 테스트
                    start_time = time.time()
                    restored_data = await backup_manager.restore_data(backup_record.backup_id)
                    restore_time = time.time() - start_time
                    
                    # 결과 검증
                    data_match = restored_data == dataset["data"]
                    compression_achieved = backup_record.compression_ratio
                    
                    result = {
                        "dataset": dataset['name'],
                        "backup_success": True,
                        "restore_success": data_match,
                        "backup_time": round(backup_time, 3),
                        "restore_time": round(restore_time, 3),
                        "compression_ratio": round(compression_achieved, 1),
                        "original_size": backup_record.original_size_bytes,
                        "compressed_size": backup_record.compressed_size_bytes
                    }
                    
                    backup_results.append(result)
                    
                    if data_match:
                        logger.info(f"✅ {dataset['name']}: 백업/복원 성공 (압축률: {compression_achieved:.1f}%, 백업: {backup_time:.3f}s, 복원: {restore_time:.3f}s)")
                        self.stats["passed_tests"] += 1
                    else:
                        logger.error(f"❌ {dataset['name']}: 복원 데이터 불일치")
                        self.stats["failed_tests"] += 1
                        
                else:
                    logger.error(f"❌ {dataset['name']}: 백업 실패")
                    self.stats["failed_tests"] += 1
                    
                self.stats["total_tests"] += 1
            
            # 백업 매니저 통계
            backup_stats = backup_manager.get_backup_statistics()
            logger.info(f"📊 백업 매니저 통계: {backup_stats}")
            
            self.test_results["backup_recovery"] = {
                "test_datasets": len(test_datasets),
                "successful_backups": len([r for r in backup_results if r["backup_success"]]),
                "successful_restores": len([r for r in backup_results if r["restore_success"]]),
                "average_compression": round(sum(r["compression_ratio"] for r in backup_results) / len(backup_results), 1) if backup_results else 0,
                "backup_results": backup_results
            }
            
            logger.info("✅ 백업 및 복구 시스템 테스트 완료")
            
        except Exception as e:
            logger.error(f"❌ 백업 및 복구 시스템 테스트 실패: {e}")
            self.stats["total_tests"] += 1
            self.stats["failed_tests"] += 1
            self.stats["errors"].append(f"백업 복구 시스템 오류: {e}")
    
    def test_compression_policy(self):
        """데이터 압축 및 보관 정책 테스트"""
        logger.info("🗜️ 6. 데이터 압축 및 보관 정책 테스트")
        
        try:
            from app.archiving.backup_manager import CompressionHandler
            from app.archiving.archival_policies import CompressionType
            
            # 테스트 데이터 생성 (다양한 패턴)
            test_data_sets = {
                "json_repetitive": {
                    "data": json.dumps({"items": [{"id": i, "name": f"item_{i}", "description": "반복적인 설명 데이터"} for i in range(500)]}),
                    "type": "반복적 JSON"
                },
                "json_random": {
                    "data": json.dumps({"data": [f"random_string_{i}_{hash(str(i))}" for i in range(100)]}),
                    "type": "랜덤 JSON"
                },
                "text_repetitive": {
                    "data": "이것은 반복적인 텍스트 데이터입니다. " * 1000,
                    "type": "반복적 텍스트"
                },
                "binary_like": {
                    "data": "".join([chr(i % 256) for i in range(10000)]),
                    "type": "바이너리 형태"
                }
            }
            
            compression_handler = CompressionHandler()
            compression_results = []
            
            # 각 압축 방식별 테스트
            compression_types = [
                (CompressionType.GZIP, "GZIP"),
                (CompressionType.BZIP2, "BZIP2"), 
                (CompressionType.LZMA, "LZMA")
            ]
            
            logger.info("📊 압축 방식별 효율성 테스트:")
            
            for comp_type, comp_name in compression_types:
                logger.info(f"\n🗜️ {comp_name} 압축 테스트:")
                
                type_results = []
                
                for data_name, data_info in test_data_sets.items():
                    original_bytes = data_info["data"].encode('utf-8')
                    original_size = len(original_bytes)
                    
                    try:
                        # 압축
                        compressor = compression_handler.get_compressor(comp_type, 6)
                        start_time = time.time()
                        compressed_data = compressor(original_bytes)
                        compress_time = time.time() - start_time
                        
                        compressed_size = len(compressed_data)
                        compression_ratio = (1 - compressed_size / original_size) * 100
                        
                        # 압축 해제
                        decompressor = compression_handler.get_decompressor(comp_type)
                        start_time = time.time()
                        decompressed_data = decompressor(compressed_data)
                        decompress_time = time.time() - start_time
                        
                        # 검증
                        data_integrity = decompressed_data == original_bytes
                        
                        result = {
                            "data_type": data_info["type"],
                            "original_size": original_size,
                            "compressed_size": compressed_size,
                            "compression_ratio": round(compression_ratio, 1),
                            "compress_time": round(compress_time * 1000, 2),  # ms
                            "decompress_time": round(decompress_time * 1000, 2),  # ms
                            "integrity_check": data_integrity
                        }
                        
                        type_results.append(result)
                        
                        status = "✅" if data_integrity else "❌"
                        logger.info(f"  {status} {data_info['type']}: {original_size:,} → {compressed_size:,} bytes ({compression_ratio:.1f}% 압축, {compress_time*1000:.1f}ms)")
                        
                        if data_integrity:
                            self.stats["passed_tests"] += 1
                        else:
                            self.stats["failed_tests"] += 1
                            self.stats["errors"].append(f"{comp_name} 압축 무결성 실패: {data_name}")
                            
                        self.stats["total_tests"] += 1
                        
                    except Exception as e:
                        logger.error(f"  ❌ {data_info['type']}: 압축 오류 - {e}")
                        self.stats["failed_tests"] += 1
                        self.stats["total_tests"] += 1
                        self.stats["errors"].append(f"{comp_name} 압축 오류: {data_name} - {e}")
                
                compression_results.append({
                    "compression_type": comp_name,
                    "results": type_results,
                    "average_ratio": round(sum(r["compression_ratio"] for r in type_results) / len(type_results), 1) if type_results else 0,
                    "average_compress_time": round(sum(r["compress_time"] for r in type_results) / len(type_results), 2) if type_results else 0
                })
            
            # 압축 방식 비교 분석
            logger.info("\n📊 압축 방식 비교 분석:")
            for result in compression_results:
                logger.info(f"  - {result['compression_type']}: 평균 압축률 {result['average_ratio']}%, 평균 시간 {result['average_compress_time']}ms")
            
            # 권장 압축 정책 생성
            best_compression = max(compression_results, key=lambda x: x['average_ratio'])
            fastest_compression = min(compression_results, key=lambda x: x['average_compress_time'])
            
            logger.info(f"\n💡 권장 압축 정책:")
            logger.info(f"  - 최고 압축률: {best_compression['compression_type']} ({best_compression['average_ratio']}%)")
            logger.info(f"  - 최고 속도: {fastest_compression['compression_type']} ({fastest_compression['average_compress_time']}ms)")
            
            self.test_results["compression_policy"] = {
                "compression_results": compression_results,
                "best_compression": best_compression['compression_type'],
                "fastest_compression": fastest_compression['compression_type'],
                "data_types_tested": len(test_data_sets)
            }
            
            logger.info("✅ 데이터 압축 및 보관 정책 테스트 완료")
            
        except Exception as e:
            logger.error(f"❌ 압축 정책 테스트 실패: {e}")
            self.stats["total_tests"] += 1
            self.stats["failed_tests"] += 1
            self.stats["errors"].append(f"압축 정책 오류: {e}")
    
    def generate_final_report(self):
        """최종 테스트 보고서 생성"""
        end_time = datetime.now()
        duration = end_time - self.start_time
        
        success_rate = round(self.stats["passed_tests"] / self.stats["total_tests"] * 100, 2) if self.stats["total_tests"] > 0 else 0
        
        logger.info("\n" + "="*80)
        logger.info("📋 API 저장 정책 및 아카이빙 시스템 종합 테스트 보고서")
        logger.info("="*80)
        
        logger.info(f"🕐 테스트 실행 시간: {duration.total_seconds():.2f}초")
        logger.info(f"📊 전체 테스트: {self.stats['total_tests']}개")
        logger.info(f"✅ 성공: {self.stats['passed_tests']}개")
        logger.info(f"❌ 실패: {self.stats['failed_tests']}개")
        logger.info(f"📈 성공률: {success_rate}%")
        
        if self.stats["errors"]:
            logger.info(f"\n❌ 오류 목록 ({len(self.stats['errors'])}개):")
            for i, error in enumerate(self.stats["errors"][:10], 1):  # 최대 10개만 표시
                logger.info(f"  {i}. {error}")
            if len(self.stats["errors"]) > 10:
                logger.info(f"  ... 및 {len(self.stats['errors']) - 10}개 추가 오류")
        
        logger.info("\n📊 테스트 결과 요약:")
        
        for test_name, result in self.test_results.items():
            logger.info(f"\n🔸 {test_name}:")
            if isinstance(result, dict):
                for key, value in result.items():
                    if key != "backup_results":  # 너무 긴 데이터는 제외
                        logger.info(f"   - {key}: {value}")
        
        # 권장사항 생성
        logger.info("\n💡 권장사항:")
        
        if success_rate >= 90:
            logger.info("✅ 시스템이 안정적으로 작동하고 있습니다.")
        elif success_rate >= 70:
            logger.info("⚠️ 일부 개선이 필요하지만 전반적으로 양호합니다.")
        else:
            logger.info("🚨 시스템에 문제가 있습니다. 즉시 점검이 필요합니다.")
        
        # 저장 공간 최적화 권장사항
        if "storage_optimization" in self.test_results:
            storage_result = self.test_results["storage_optimization"]
            if storage_result.get("optimizable_data_mb", 0) > 1000:  # 1GB 이상
                logger.info(f"💾 {storage_result['optimizable_data_mb']}MB의 데이터 최적화가 가능합니다.")
        
        # 압축 정책 권장사항
        if "compression_policy" in self.test_results:
            comp_result = self.test_results["compression_policy"]
            logger.info(f"🗜️ 최적 압축 방식: {comp_result.get('best_compression', 'GZIP')}")
        
        logger.info("\n🎉 종합 테스트 완료!")
        logger.info("="*80)
        
        # 결과를 파일로 저장
        report_file = f"test_results/api_storage_comprehensive_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        os.makedirs("test_results", exist_ok=True)
        
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump({
                "test_info": {
                    "start_time": self.start_time.isoformat(),
                    "end_time": end_time.isoformat(),
                    "duration_seconds": duration.total_seconds(),
                    "success_rate": success_rate
                },
                "statistics": self.stats,
                "test_results": self.test_results
            }, f, ensure_ascii=False, indent=2, default=str)
        
        logger.info(f"📄 상세 보고서: {report_file}")
    
    def cleanup(self):
        """테스트 정리"""
        if self.temp_backup_dir and os.path.exists(self.temp_backup_dir):
            shutil.rmtree(self.temp_backup_dir, ignore_errors=True)
            logger.info(f"🧹 임시 디렉토리 정리 완료: {self.temp_backup_dir}")


def main():
    """메인 함수"""
    test_runner = ComprehensiveAPIStorageTest()
    test_runner.run_all_tests()


if __name__ == "__main__":
    main()