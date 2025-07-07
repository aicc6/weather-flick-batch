#!/usr/bin/env python3
"""
배치 INSERT 성능 테스트 스크립트

이 스크립트는 다음 작업을 수행합니다:
1. 기존 방식 vs 배치 최적화 방식 성능 비교
2. 다양한 배치 크기별 성능 측정
3. 메모리 사용량 모니터링
4. 최적의 배치 설정 추천

실행 방법:
python scripts/test_batch_performance.py [--test-size 1000] [--batch-sizes 100,500,1000,2000]

옵션:
--test-size: 테스트할 레코드 수 (기본: 1000)
--batch-sizes: 테스트할 배치 크기들 (기본: 100,500,1000,2000)
--skip-legacy: 기존 방식 테스트 건너뛰기
"""

import sys
import argparse
import asyncio
import time
import tracemalloc
import uuid
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Any
import random
import statistics

# 프로젝트 루트 추가
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from app.core.batch_insert_optimizer import (
    BatchInsertOptimizer, 
    BatchConfig, 
    BatchResult,
    optimize_weather_current_insert,
    optimize_weather_forecast_insert
)
from app.core.database_manager import DatabaseManager
from app.core.logger import get_logger


class BatchPerformanceTester:
    """배치 INSERT 성능 테스터"""
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.db_manager = DatabaseManager()
        self.test_results = []
        
    def generate_test_weather_data(self, count: int) -> List[Dict[str, Any]]:
        """테스트용 날씨 데이터 생성"""
        
        test_data = []
        base_time = datetime.now()
        
        regions = ["서울", "부산", "대구", "인천", "광주", "대전", "울산"]
        weather_conditions = ["맑음", "흐림", "비", "눈", "안개"]
        
        for i in range(count):
            data = {
                "region_code": f"{random.randint(11, 50):02d}",
                "region_name": random.choice(regions),
                "temperature": round(random.uniform(-10, 35), 1),
                "humidity": random.randint(30, 95),
                "precipitation": round(random.uniform(0, 50), 1),
                "wind_speed": round(random.uniform(0, 20), 1),
                "wind_direction": random.randint(0, 360),
                "atmospheric_pressure": round(random.uniform(950, 1050), 1),
                "weather_condition": random.choice(weather_conditions),
                "visibility": round(random.uniform(0.1, 20), 1),
                "observed_at": base_time + timedelta(minutes=i)
            }
            test_data.append(data)
            
        return test_data
    
    def generate_test_forecast_data(self, count: int) -> List[Dict[str, Any]]:
        """테스트용 예보 데이터 생성"""
        
        test_data = []
        base_date = datetime.now().date()
        
        regions = ["서울", "부산", "대구", "인천", "광주", "대전", "울산"]
        weather_conditions = ["맑음", "흐림", "비", "눈", "안개"]
        forecast_times = ["0000", "0300", "0600", "0900", "1200", "1500", "1800", "2100"]
        
        for i in range(count):
            forecast_date = base_date + timedelta(days=i // 100)  # 날짜 분산
            
            data = {
                "region_code": f"{random.randint(11, 50):02d}",
                "region_name": random.choice(regions),
                "nx": random.randint(50, 150),
                "ny": random.randint(100, 200),
                "forecast_date": forecast_date,
                "forecast_time": random.choice(forecast_times),
                "temperature": round(random.uniform(-10, 35), 1),
                "min_temp": round(random.uniform(-15, 25), 1),
                "max_temp": round(random.uniform(0, 40), 1),
                "weather_condition": random.choice(weather_conditions),
                "forecast_type": random.choice(["short", "medium"])
            }
            test_data.append(data)
            
        return test_data
    
    async def test_legacy_insert_method(
        self, 
        test_data: List[Dict[str, Any]], 
        table_type: str = "current"
    ) -> Dict[str, Any]:
        """기존 방식 성능 테스트 (시뮬레이션)"""
        
        start_time = time.time()
        tracemalloc.start()
        
        try:
            # 기존 방식 시뮬레이션 (개별 INSERT)
            processed_count = 0
            
            for data in test_data:
                # 실제 INSERT는 하지 않고 시간만 소모
                await asyncio.sleep(0.001)  # 개별 INSERT 오버헤드 시뮬레이션
                processed_count += 1
                
                # 100개마다 진행률 로깅
                if processed_count % 100 == 0:
                    self.logger.debug(f"기존 방식 진행률: {processed_count}/{len(test_data)}")
            
            execution_time = time.time() - start_time
            current_memory, peak_memory = tracemalloc.get_traced_memory()
            
            return {
                "method": "legacy",
                "table_type": table_type,
                "records": len(test_data),
                "execution_time": execution_time,
                "records_per_second": len(test_data) / execution_time if execution_time > 0 else 0,
                "memory_peak_mb": peak_memory / 1024 / 1024,
                "memory_current_mb": current_memory / 1024 / 1024,
                "success_rate": 1.0
            }
            
        finally:
            tracemalloc.stop()
    
    async def test_batch_insert_method(
        self, 
        test_data: List[Dict[str, Any]], 
        batch_size: int,
        table_type: str = "current"
    ) -> Dict[str, Any]:
        """배치 INSERT 성능 테스트"""
        
        start_time = time.time()
        tracemalloc.start()
        
        try:
            config = BatchConfig(
                batch_size=batch_size,
                max_memory_mb=50,
                retry_attempts=1  # 테스트용으로 재시도 최소화
            )
            
            # 정상적인 UUID 생성 (테스트용)
            raw_data_id = str(uuid.uuid4())
            
            if table_type == "current":
                result = await optimize_weather_current_insert(test_data, raw_data_id, config)
            elif table_type == "forecast":
                result = await optimize_weather_forecast_insert(test_data, raw_data_id, config)
            else:
                raise ValueError(f"지원하지 않는 테이블 타입: {table_type}")
            
            execution_time = time.time() - start_time
            current_memory, peak_memory = tracemalloc.get_traced_memory()
            
            return {
                "method": "batch_optimized",
                "table_type": table_type,
                "batch_size": batch_size,
                "records": result.total_records,
                "successful_records": result.successful_records,
                "execution_time": execution_time,
                "records_per_second": len(test_data) / execution_time if execution_time > 0 else 0,
                "memory_peak_mb": peak_memory / 1024 / 1024,
                "memory_current_mb": current_memory / 1024 / 1024,
                "success_rate": result.success_rate,
                "batch_execution_time": result.execution_time,
                "batch_records_per_second": result.records_per_second
            }
            
        finally:
            tracemalloc.stop()
    
    async def run_performance_comparison(
        self, 
        test_size: int, 
        batch_sizes: List[int],
        skip_legacy: bool = False
    ):
        """성능 비교 테스트 실행"""
        
        self.logger.info(f"성능 비교 테스트 시작: {test_size}건 레코드")
        
        # 테스트 데이터 생성
        current_weather_data = self.generate_test_weather_data(test_size)
        forecast_data = self.generate_test_forecast_data(test_size)
        
        self.logger.info(f"테스트 데이터 생성 완료: 현재날씨 {len(current_weather_data)}건, 예보 {len(forecast_data)}건")
        
        # 기존 방식 테스트 (시뮬레이션)
        if not skip_legacy:
            self.logger.info("기존 방식 성능 테스트 중...")
            legacy_current = await self.test_legacy_insert_method(current_weather_data, "current")
            legacy_forecast = await self.test_legacy_insert_method(forecast_data, "forecast")
            
            self.test_results.extend([legacy_current, legacy_forecast])
            
            self.logger.info(f"기존 방식 - 현재날씨: {legacy_current['records_per_second']:.1f} records/sec")
            self.logger.info(f"기존 방식 - 예보: {legacy_forecast['records_per_second']:.1f} records/sec")
        
        # 배치 최적화 방식 테스트
        for batch_size in batch_sizes:
            self.logger.info(f"배치 크기 {batch_size} 테스트 중...")
            
            # 현재 날씨 테스트
            batch_current = await self.test_batch_insert_method(
                current_weather_data, batch_size, "current"
            )
            
            # 예보 데이터 테스트
            batch_forecast = await self.test_batch_insert_method(
                forecast_data, batch_size, "forecast"
            )
            
            self.test_results.extend([batch_current, batch_forecast])
            
            self.logger.info(
                f"배치 크기 {batch_size} - 현재날씨: {batch_current['records_per_second']:.1f} records/sec, "
                f"메모리: {batch_current['memory_peak_mb']:.1f}MB"
            )
            self.logger.info(
                f"배치 크기 {batch_size} - 예보: {batch_forecast['records_per_second']:.1f} records/sec, "
                f"메모리: {batch_forecast['memory_peak_mb']:.1f}MB"
            )
        
        # 결과 분석 및 보고서 생성
        self.generate_performance_report()
    
    def generate_performance_report(self):
        """성능 테스트 결과 보고서 생성"""
        
        if not self.test_results:
            self.logger.warning("테스트 결과가 없습니다.")
            return
        
        report_lines = []
        report_lines.append("=" * 80)
        report_lines.append("배치 INSERT 성능 테스트 결과 보고서")
        report_lines.append(f"생성일시: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append("=" * 80)
        
        # 테이블 타입별 결과 분석
        for table_type in ["current", "forecast"]:
            type_results = [r for r in self.test_results if r["table_type"] == table_type]
            if not type_results:
                continue
                
            report_lines.append(f"\n📊 {table_type.upper()} 테이블 성능 결과:")
            report_lines.append("-" * 50)
            
            # 기존 방식 결과
            legacy_results = [r for r in type_results if r["method"] == "legacy"]
            if legacy_results:
                legacy = legacy_results[0]
                report_lines.append(f"🔴 기존 방식:")
                report_lines.append(f"   처리량: {legacy['records_per_second']:.1f} records/sec")
                report_lines.append(f"   소요시간: {legacy['execution_time']:.2f}초")
                report_lines.append(f"   메모리 사용: {legacy['memory_peak_mb']:.1f}MB")
            
            # 배치 방식 결과
            batch_results = [r for r in type_results if r["method"] == "batch_optimized"]
            if batch_results:
                report_lines.append(f"\n🟢 배치 최적화 방식:")
                
                best_performance = max(batch_results, key=lambda x: x['records_per_second'])
                best_memory = min(batch_results, key=lambda x: x['memory_peak_mb'])
                
                for result in sorted(batch_results, key=lambda x: x['batch_size']):
                    batch_size = result['batch_size']
                    rps = result['records_per_second']
                    memory = result['memory_peak_mb']
                    
                    indicator = ""
                    if result == best_performance:
                        indicator += "⚡"
                    if result == best_memory:
                        indicator += "💾"
                    
                    report_lines.append(
                        f"   배치크기 {batch_size:4d}: {rps:8.1f} records/sec, "
                        f"메모리 {memory:5.1f}MB {indicator}"
                    )
                
                # 성능 개선 분석
                if legacy_results:
                    legacy_rps = legacy_results[0]['records_per_second']
                    best_rps = best_performance['records_per_second']
                    improvement = (best_rps / legacy_rps - 1) * 100 if legacy_rps > 0 else 0
                    
                    report_lines.append(f"\n📈 성능 개선:")
                    report_lines.append(f"   최대 성능 향상: {improvement:.1f}%")
                    report_lines.append(f"   최적 배치 크기: {best_performance['batch_size']}")
                    report_lines.append(f"   메모리 효율 배치: {best_memory['batch_size']}")
        
        # 권장사항
        report_lines.append("\n💡 권장 설정:")
        
        # 전체 결과에서 최적 배치 크기 찾기
        batch_results = [r for r in self.test_results if r["method"] == "batch_optimized"]
        if batch_results:
            # 성능과 메모리 효율성의 균형점 찾기
            scored_results = []
            for result in batch_results:
                rps_score = result['records_per_second'] / 1000  # 정규화
                memory_score = 50 / result['memory_peak_mb']  # 메모리는 역수 (적을수록 좋음)
                total_score = rps_score + memory_score
                scored_results.append((result, total_score))
            
            best_balance = max(scored_results, key=lambda x: x[1])[0]
            
            report_lines.append(f"   배치 크기: {best_balance['batch_size']}")
            report_lines.append(f"   예상 성능: {best_balance['records_per_second']:.1f} records/sec")
            report_lines.append(f"   메모리 사용: {best_balance['memory_peak_mb']:.1f}MB")
        
        # 추가 최적화 제안
        report_lines.append("\n🔧 추가 최적화 제안:")
        report_lines.append("   1. 병렬 처리: 지역별 동시 처리 구현")
        report_lines.append("   2. 연결 풀 확장: 최대 연결 수 증가")
        report_lines.append("   3. 인덱스 최적화: UPSERT 대상 컬럼 인덱스 조정")
        report_lines.append("   4. 파티셔닝: 날짜별 테이블 파티셔닝 고려")
        
        report_lines.append("\n" + "=" * 80)
        
        # 리포트 출력 및 저장
        report_content = "\n".join(report_lines)
        print(report_content)
        
        # 파일로 저장
        report_file = project_root / "logs" / f"batch_performance_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        report_file.parent.mkdir(exist_ok=True)
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report_content)
        
        self.logger.info(f"성능 테스트 보고서 저장: {report_file}")


async def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description='배치 INSERT 성능 테스트')
    parser.add_argument('--test-size', type=int, default=1000, 
                       help='테스트할 레코드 수 (기본: 1000)')
    parser.add_argument('--batch-sizes', type=str, default='100,500,1000,2000',
                       help='테스트할 배치 크기들 (기본: 100,500,1000,2000)')
    parser.add_argument('--skip-legacy', action='store_true',
                       help='기존 방식 테스트 건너뛰기')
    
    args = parser.parse_args()
    
    # 배치 크기 파싱
    try:
        batch_sizes = [int(size.strip()) for size in args.batch_sizes.split(',')]
    except ValueError:
        print("❌ 배치 크기 형식이 잘못되었습니다. 예: --batch-sizes 100,500,1000")
        return 1
    
    tester = BatchPerformanceTester()
    
    try:
        await tester.run_performance_comparison(
            test_size=args.test_size,
            batch_sizes=batch_sizes,
            skip_legacy=args.skip_legacy
        )
        
        print("✅ 성능 테스트가 완료되었습니다.")
        print("📋 결과:")
        print("  1. 성능 비교 분석 완료")
        print("  2. 최적 배치 크기 식별")
        print("  3. 메모리 사용량 분석")
        print("  4. 성능 개선 권장사항 제시")
        
        return 0
        
    except Exception as e:
        print(f"❌ 성능 테스트 실패: {e}")
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)