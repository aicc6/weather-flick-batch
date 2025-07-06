"""
데이터 품질 엔진 실제 테스트 스크립트

실제 데이터베이스 데이터를 사용하여 품질 엔진의 성능과 기능을 테스트합니다.
"""

import asyncio
import logging
import json
from datetime import datetime
from pathlib import Path
import sys
import os

# 프로젝트 루트 경로 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from app.quality import (
    DataQualityEngine,
    QualityConfig,
    QualityAction,
    DuplicateConfig,
    DuplicateStrategy,
    DuplicateType,
    ValidationSeverity
)
# from app.core.database import get_db_connection


# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/quality_test.log')
    ]
)

logger = logging.getLogger(__name__)


class QualityEngineTestRunner:
    """품질 엔진 테스트 실행기"""
    
    def __init__(self):
        self.results_dir = Path("test_results/quality")
        self.results_dir.mkdir(parents=True, exist_ok=True)
        
    async def run_all_tests(self):
        """모든 테스트 실행"""
        
        logger.info("🧪 데이터 품질 엔진 테스트 시작")
        
        try:
            # 1. 기본 품질 검사 테스트
            await self.test_basic_quality_check()
            
            # 2. 관광지 데이터 품질 테스트
            await self.test_tourist_attractions_quality()
            
            # 3. 날씨 데이터 품질 테스트
            await self.test_weather_data_quality()
            
            # 4. 중복 감지 테스트
            await self.test_duplicate_detection()
            
            # 5. 데이터 정리 테스트
            await self.test_data_cleaning()
            
            # 6. 종합 품질 보고서 테스트
            await self.test_comprehensive_quality_report()
            
            logger.info("✅ 모든 품질 엔진 테스트 완료")
            
        except Exception as e:
            logger.error(f"❌ 테스트 실행 중 오류: {e}")
            raise
    
    async def test_basic_quality_check(self):
        """기본 품질 검사 테스트"""
        
        logger.info("📊 기본 품질 검사 테스트 시작")
        
        # 테스트 데이터 생성
        test_data = [
            {
                "id": 1,
                "name": "경복궁",
                "latitude": 37.579617,
                "longitude": 126.977041,
                "phone": "02-3700-3900",
                "email": "info@royalpalace.go.kr",
                "created_date": "2024-01-15"
            },
            {
                "id": 2,
                "name": "",  # 빈 이름
                "latitude": 37.582219,
                "longitude": 126.991156,
                "phone": "invalid-phone",  # 잘못된 전화번호
                "email": "invalid-email",  # 잘못된 이메일
                "created_date": "invalid-date"  # 잘못된 날짜
            },
            {
                "id": 3,
                "name": "창덕궁",
                "latitude": 200.0,  # 잘못된 위도
                "longitude": -200.0,  # 잘못된 경도
                "phone": None,
                "email": None,
                "created_date": None
            }
        ]
        
        # 품질 엔진 설정
        config = QualityConfig(
            action=QualityAction.FULL_PROCESSING,
            required_fields=["id", "name", "latitude", "longitude"],
            max_error_rate=0.3,
            min_completeness=0.7
        )
        
        # 품질 엔진 실행
        engine = DataQualityEngine(config)
        report = await engine.process_dataset(test_data, "basic_test")
        
        # 결과 출력
        self._print_quality_summary(report)
        
        # 보고서 저장
        report_file = self.results_dir / f"basic_quality_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        engine.export_report_to_json(report, str(report_file))
        
        logger.info(f"기본 품질 검사 결과 저장: {report_file}")
    
    async def test_tourist_attractions_quality(self):
        """관광지 데이터 품질 테스트"""
        
        logger.info("🏛️ 관광지 데이터 품질 테스트 시작")
        
        try:
            # 샘플 관광지 데이터 생성 (실제 DB 연결 대신)
            attractions_data = [
                {
                    "id": 1,
                    "name": "경복궁",
                    "description": "조선시대 정궁",
                    "latitude": 37.579617,
                    "longitude": 126.977041,
                    "phone": "02-3700-3900",
                    "homepage": "http://www.royalpalace.go.kr",
                    "address": "서울시 종로구",
                    "created_at": "2024-01-15"
                },
                {
                    "id": 2,
                    "name": " 창덕궁 ",  # 공백 있음
                    "description": "<p>유네스코 세계문화유산</p>",  # HTML 태그
                    "latitude": 37.582219,
                    "longitude": 126.991156,
                    "phone": "02) 762-8261",  # 비정규화된 전화번호
                    "homepage": "www.cdg.go.kr",  # 프로토콜 없음
                    "address": "서울시 종로구",
                    "created_at": "2024/01/16"
                },
                {
                    "id": 3,
                    "name": "경복궁",  # 중복된 이름
                    "description": "조선시대 정궁",
                    "latitude": 37.579617,
                    "longitude": 126.977041,
                    "phone": "",  # 빈 값
                    "homepage": None,
                    "address": "",
                    "created_at": "invalid-date"
                }
            ]
            
            # 품질 엔진 설정 (관광지 데이터 특화)
            config = QualityConfig(
                action=QualityAction.FULL_PROCESSING,
                required_fields=["id", "name", "latitude", "longitude"],
                duplicate_config=DuplicateConfig(
                    strategy=DuplicateStrategy.KEEP_FIRST,
                    duplicate_type=DuplicateType.FIELD_BASED,
                    key_fields=["name", "latitude", "longitude"]
                ),
                max_error_rate=0.1,
                min_completeness=0.8
            )
            
            # 품질 엔진 실행
            engine = DataQualityEngine(config)
            report = await engine.process_dataset(attractions_data, "tourist_attractions")
            
            # 결과 출력
            self._print_quality_summary(report)
            
            # 상세 분석
            self._analyze_field_quality(report, "관광지")
            
            # 보고서 저장
            report_file = self.results_dir / f"attractions_quality_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            engine.export_report_to_json(report, str(report_file))
            
            logger.info(f"관광지 품질 테스트 결과 저장: {report_file}")
            
        except Exception as e:
            logger.error(f"관광지 데이터 품질 테스트 오류: {e}")
    
    async def test_weather_data_quality(self):
        """날씨 데이터 품질 테스트"""
        
        logger.info("🌤️ 날씨 데이터 품질 테스트 시작")
        
        try:
            # 샘플 날씨 데이터 생성 (실제 DB 연결 대신)
            weather_data = [
                {
                    "id": 1,
                    "location_id": 1,
                    "forecast_date": "2024-01-15",
                    "temperature": 15.5,
                    "humidity": 65,
                    "precipitation": 0.0,
                    "wind_speed": 2.3,
                    "weather_condition": "맑음",
                    "created_at": "2024-01-15"
                },
                {
                    "id": 2,
                    "location_id": 2,
                    "forecast_date": "2024-01-16",
                    "temperature": 200.0,  # 잘못된 온도
                    "humidity": 150,  # 잘못된 습도
                    "precipitation": -5.0,  # 음수 강수량
                    "wind_speed": 1000.0,  # 비현실적인 풍속
                    "weather_condition": "비",
                    "created_at": "2024-01-16"
                },
                {
                    "id": 3,
                    "location_id": None,  # 필수 필드 누락
                    "forecast_date": "invalid-date",  # 잘못된 날짜
                    "temperature": None,  # 필수 필드 누락
                    "humidity": 75,
                    "precipitation": 2.5,
                    "wind_speed": 4.2,
                    "weather_condition": "",  # 빈 값
                    "created_at": "2024-01-17"
                }
            ]
            
            # 품질 엔진 설정 (날씨 데이터 특화)
            config = QualityConfig(
                action=QualityAction.CLEAN_AND_VALIDATE,
                required_fields=["location_id", "forecast_date", "temperature"],
                max_error_rate=0.05,  # 날씨 데이터는 더 엄격
                min_completeness=0.95
            )
            
            # 커스텀 검증 규칙 추가
            engine = DataQualityEngine(config)
            
            # 온도 범위 검증
            engine.validator.add_custom_rule(
                field_name="temperature",
                rule_name="temperature_range",
                validator_func=lambda temp: -50 <= float(temp) <= 60 if temp is not None else True,
                severity=ValidationSeverity.ERROR,
                error_message="온도는 -50°C ~ 60°C 범위여야 합니다."
            )
            
            # 습도 범위 검증
            engine.validator.add_custom_rule(
                field_name="humidity",
                rule_name="humidity_range", 
                validator_func=lambda hum: 0 <= float(hum) <= 100 if hum is not None else True,
                severity=ValidationSeverity.ERROR,
                error_message="습도는 0% ~ 100% 범위여야 합니다."
            )
            
            # 품질 엔진 실행
            report = await engine.process_dataset(weather_data, "weather_forecast")
            
            # 결과 출력
            self._print_quality_summary(report)
            
            # 날씨 데이터 특화 분석
            self._analyze_weather_quality(report)
            
            # 보고서 저장
            report_file = self.results_dir / f"weather_quality_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            engine.export_report_to_json(report, str(report_file))
            
            logger.info(f"날씨 품질 테스트 결과 저장: {report_file}")
            
        except Exception as e:
            logger.error(f"날씨 데이터 품질 테스트 오류: {e}")
    
    async def test_duplicate_detection(self):
        """중복 감지 테스트"""
        
        logger.info("🔄 중복 감지 테스트 시작")
        
        # 중복이 있는 테스트 데이터 생성
        test_data = [
            {"id": 1, "name": "경복궁", "lat": 37.579617, "lng": 126.977041},
            {"id": 2, "name": "창덕궁", "lat": 37.582219, "lng": 126.991156},
            {"id": 3, "name": "경복궁", "lat": 37.579617, "lng": 126.977041},  # 완전 중복
            {"id": 4, "name": "경복궁", "lat": 37.579620, "lng": 126.977040},  # 유사 중복 (이름 같음)
            {"id": 5, "name": "덕수궁", "lat": 37.565872, "lng": 126.975020},
            {"id": 6, "name": "덕수궁", "lat": 37.565872, "lng": 126.975020},  # 완전 중복
        ]
        
        # 여러 중복 감지 전략 테스트
        strategies = [
            (DuplicateStrategy.KEEP_FIRST, "첫 번째 유지"),
            (DuplicateStrategy.KEEP_LAST, "마지막 유지"),
            (DuplicateStrategy.MARK_ONLY, "표시만")
        ]
        
        for strategy, desc in strategies:
            logger.info(f"중복 처리 전략 테스트: {desc}")
            
            config = QualityConfig(
                action=QualityAction.REMOVE_DUPLICATES,
                duplicate_config=DuplicateConfig(
                    strategy=strategy,
                    duplicate_type=DuplicateType.FIELD_BASED,
                    key_fields=["name"]
                )
            )
            
            engine = DataQualityEngine(config)
            report = await engine.process_dataset(test_data, f"duplicate_test_{strategy.value}")
            
            print(f"\n=== {desc} ===")
            print(f"원본 레코드: {len(test_data)}")
            print(f"중복 그룹: {report.metrics.duplicate_groups}")
            print(f"중복 레코드: {report.metrics.duplicate_records}")
            print(f"처리 후 레코드: {len(report.processed_dataset)}")
            print(f"중복률: {report.metrics.duplicate_rate:.1%}")
    
    async def test_data_cleaning(self):
        """데이터 정리 테스트"""
        
        logger.info("🧹 데이터 정리 테스트 시작")
        
        # 정리가 필요한 테스트 데이터
        dirty_data = [
            {
                "id": 1,
                "name": " 경복궁 ",  # 앞뒤 공백
                "phone": "02) 3700-3900",  # 비정규화된 전화번호
                "email": " INFO@ROYALPALACE.GO.KR ",  # 대문자 + 공백
                "website": "www.royalpalace.go.kr",  # 프로토콜 없는 URL
                "description": "<p>조선시대   정궁</p>",  # HTML 태그 + 중복 공백
                "price": "₩5,000",  # 통화 기호 있는 숫자
                "latitude": "37.579617123456"  # 너무 정밀한 좌표
            },
            {
                "id": 2,
                "name": "창덕궁",
                "phone": "010 1234 5678",
                "email": "test@EXAMPLE.COM",
                "website": "HTTPS://EXAMPLE.COM/",
                "description": "유네스코&nbsp;세계문화유산",  # HTML 엔티티
                "price": "약 3,000원",
                "latitude": None
            }
        ]
        
        config = QualityConfig(action=QualityAction.CLEAN_AND_VALIDATE)
        engine = DataQualityEngine(config)
        
        report = await engine.process_dataset(dirty_data, "cleaning_test")
        
        print("\n=== 데이터 정리 결과 ===")
        print(f"정리된 필드 수: {report.metrics.cleaned_fields}")
        print(f"자동 수정 수: {report.metrics.auto_fixes}")
        print(f"정리 성공률: {report.metrics.clean_success_rate:.1%}")
        
        # 정리 전후 비교
        print("\n=== 정리 전후 비교 ===")
        for i, (original, cleaned) in enumerate(zip(dirty_data, report.processed_dataset)):
            print(f"\n레코드 {i+1}:")
            for field in ["name", "phone", "email", "website"]:
                if field in original and field in cleaned:
                    orig_val = original[field]
                    clean_val = cleaned[field]
                    if orig_val != clean_val:
                        print(f"  {field}: '{orig_val}' → '{clean_val}'")
    
    async def test_comprehensive_quality_report(self):
        """종합 품질 보고서 테스트"""
        
        logger.info("📋 종합 품질 보고서 테스트 시작")
        
        # 복합적인 품질 문제가 있는 데이터
        complex_data = [
            {"id": 1, "name": "정상 데이터", "lat": 37.579617, "lng": 126.977041, "email": "test@example.com"},
            {"id": 2, "name": "", "lat": 37.582219, "lng": 126.991156, "email": "invalid-email"},  # 빈 이름, 잘못된 이메일
            {"id": 3, "name": "중복 데이터", "lat": 37.579617, "lng": 126.977041, "email": "test@example.com"},  # 위치 중복
            {"id": 4, "name": "범위 오류", "lat": 200.0, "lng": -300.0, "email": "test@example.com"},  # 좌표 범위 오류
            {"id": 5, "name": " 정리 필요 ", "lat": 37.565872, "lng": 126.975020, "email": " TEST@EXAMPLE.COM "},  # 정리 필요
            {"id": 6, "name": "중복 데이터", "lat": 37.579617, "lng": 126.977041, "email": "test@example.com"},  # 또 다른 중복
            {"id": None, "name": "ID 없음", "lat": 37.123456, "lng": 127.123456, "email": "test@example.com"},  # ID 없음
        ]
        
        config = QualityConfig(
            action=QualityAction.FULL_PROCESSING,
            required_fields=["id", "name", "lat", "lng"],
            duplicate_config=DuplicateConfig(
                strategy=DuplicateStrategy.KEEP_FIRST,
                key_fields=["lat", "lng"]
            ),
            max_error_rate=0.2,
            min_completeness=0.8
        )
        
        engine = DataQualityEngine(config)
        report = await engine.process_dataset(complex_data, "comprehensive_test")
        
        # 종합 보고서 출력
        print("\n" + "="*60)
        print("📋 종합 데이터 품질 보고서")
        print("="*60)
        
        self._print_quality_summary(report)
        
        # 권장사항 출력
        if report.recommendations:
            print("\n💡 권장사항:")
            for i, rec in enumerate(report.recommendations, 1):
                print(f"  {i}. {rec}")
        
        # 상세 오류 분석
        if report.error_summary:
            print("\n🔍 오류 상세 분석:")
            if "errors_by_field" in report.error_summary:
                print("  필드별 오류:")
                for field, count in report.error_summary["errors_by_field"].items():
                    print(f"    - {field}: {count}개")
            
            if "errors_by_type" in report.error_summary:
                print("  규칙별 오류:")
                for rule, count in report.error_summary["errors_by_type"].items():
                    print(f"    - {rule}: {count}개")
        
        # 보고서 저장
        report_file = self.results_dir / f"comprehensive_quality_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        engine.export_report_to_json(report, str(report_file))
        
        logger.info(f"종합 품질 보고서 저장: {report_file}")
    
    def _print_quality_summary(self, report):
        """품질 요약 정보 출력"""
        
        print(f"\n📊 품질 요약 - {report.dataset_name}")
        print("-" * 50)
        print(f"총 레코드: {report.metrics.total_records}")
        print(f"처리된 레코드: {report.metrics.processed_records}")
        print(f"유효한 레코드: {report.metrics.valid_records}")
        print(f"품질 점수: {report.metrics.quality_score:.2f}")
        print(f"품질 상태: {report.metrics.quality_status.value}")
        print(f"오류율: {report.metrics.error_rate:.1%}")
        print(f"완성도: {report.metrics.completeness:.1%}")
        
        if report.metrics.duplicate_rate > 0:
            print(f"중복률: {report.metrics.duplicate_rate:.1%}")
        
        if report.metrics.auto_fixes > 0:
            print(f"자동 수정: {report.metrics.auto_fixes}개")
    
    def _analyze_field_quality(self, report, data_type):
        """필드별 품질 분석"""
        
        print(f"\n🔍 {data_type} 필드별 품질 분석:")
        print("-" * 40)
        
        for field, completeness in report.metrics.field_completeness.items():
            status = "✅" if completeness >= 0.9 else "⚠️" if completeness >= 0.7 else "❌"
            print(f"{status} {field}: {completeness:.1%}")
    
    def _analyze_weather_quality(self, report):
        """날씨 데이터 특화 품질 분석"""
        
        print("\n🌤️ 날씨 데이터 품질 분석:")
        print("-" * 40)
        
        # 온도 관련 오류 확인
        temp_errors = [r for r in report.validation_results 
                      if r.field_name == "temperature" and not r.is_valid]
        if temp_errors:
            print(f"❌ 온도 데이터 오류: {len(temp_errors)}개")
        
        # 습도 관련 오류 확인
        humidity_errors = [r for r in report.validation_results 
                          if r.field_name == "humidity" and not r.is_valid]
        if humidity_errors:
            print(f"❌ 습도 데이터 오류: {len(humidity_errors)}개")
        
        # 필수 필드 완성도
        critical_fields = ["location_id", "forecast_date", "temperature"]
        for field in critical_fields:
            if field in report.metrics.field_completeness:
                completeness = report.metrics.field_completeness[field]
                status = "✅" if completeness >= 0.95 else "⚠️"
                print(f"{status} {field} 완성도: {completeness:.1%}")


async def main():
    """메인 함수"""
    
    # 로그 디렉토리 생성
    Path("logs").mkdir(exist_ok=True)
    
    # 테스트 실행기 생성
    test_runner = QualityEngineTestRunner()
    
    try:
        # 모든 테스트 실행
        await test_runner.run_all_tests()
        
        print("\n🎉 모든 품질 엔진 테스트가 성공적으로 완료되었습니다!")
        print(f"📁 테스트 결과는 {test_runner.results_dir}에 저장되었습니다.")
        
    except Exception as e:
        logger.error(f"테스트 실행 실패: {e}")
        print(f"\n❌ 테스트 실행 중 오류가 발생했습니다: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    # asyncio 이벤트 루프 실행
    exit_code = asyncio.run(main())
    sys.exit(exit_code)