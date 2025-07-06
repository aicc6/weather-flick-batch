"""
데이터 품질 시스템 테스트

품질 엔진, 검증, 중복 감지, 데이터 정리 기능을 포괄적으로 테스트합니다.
"""

import pytest
import asyncio
from datetime import datetime
from typing import List, Dict, Any

from app.quality import (
    DataQualityEngine,
    QualityConfig,
    QualityAction,
    QualityStatus,
    DataValidator,
    ValidationSeverity,
    DuplicateDetector,
    DuplicateConfig,
    DuplicateStrategy,
    DataCleaner
)


class TestDataQualityEngine:
    """데이터 품질 엔진 테스트"""
    
    @pytest.fixture
    def sample_dataset(self) -> List[Dict[str, Any]]:
        """테스트용 샘플 데이터셋"""
        return [
            {
                "id": 1,
                "name": "경복궁",
                "description": "조선 왕조의 정궁",
                "latitude": 37.579617,
                "longitude": 126.977041,
                "phone": "02-3700-3900",
                "email": "info@royalpalace.go.kr",
                "website": "http://www.royalpalace.go.kr",
                "created_date": "2024-01-15"
            },
            {
                "id": 2,
                "name": " 창덕궁 ",  # 공백 있음
                "description": "<p>유네스코 세계문화유산</p>",  # HTML 태그
                "latitude": 37.582219,
                "longitude": 126.991156,
                "phone": "02) 762-8261",  # 형식 비정규화
                "email": "CHANGDEOK@CHA.GO.KR",  # 대문자
                "website": "www.cdg.go.kr",  # 프로토콜 없음
                "created_date": "2024/01/16"  # 다른 날짜 형식
            },
            {
                "id": 3,
                "name": "경복궁",  # 중복된 이름
                "description": "조선 왕조의 정궁",  # 중복된 설명
                "latitude": 37.579617,
                "longitude": 126.977041,
                "phone": "",  # 빈 값
                "email": "invalid-email",  # 잘못된 이메일
                "website": None,  # null 값
                "created_date": "invalid-date"  # 잘못된 날짜
            },
            {
                "id": 4,
                "name": "덕수궁",
                "description": "대한제국 황궁",
                "latitude": 37.565872,
                "longitude": 126.975020,
                "phone": "010-1234-5678",
                "email": "test@example.com",
                "website": "https://www.deoksugung.go.kr",
                "created_date": "2024-01-17"
            }
        ]
    
    @pytest.fixture
    def quality_config(self) -> QualityConfig:
        """테스트용 품질 설정"""
        return QualityConfig(
            action=QualityAction.FULL_PROCESSING,
            max_error_rate=0.1,
            max_warning_rate=0.3,
            min_completeness=0.8,
            required_fields=["id", "name", "latitude", "longitude"],
            auto_fix_enabled=True
        )
    
    @pytest.mark.asyncio
    async def test_full_quality_processing(self, sample_dataset, quality_config):
        """전체 품질 처리 테스트"""
        
        # 품질 엔진 생성
        engine = DataQualityEngine(quality_config)
        
        # 데이터 처리
        report = await engine.process_dataset(
            dataset=sample_dataset,
            dataset_name="test_attractions"
        )
        
        # 기본 검증
        assert report.report_id is not None
        assert report.dataset_name == "test_attractions"
        assert report.metrics.total_records == len(sample_dataset)
        assert report.metrics.processed_records > 0
        
        # 품질 상태 확인
        assert report.metrics.quality_status in [
            QualityStatus.POOR, QualityStatus.FAIR, 
            QualityStatus.GOOD, QualityStatus.EXCELLENT
        ]
        
        # 처리된 데이터 확인
        assert len(report.processed_dataset) <= len(sample_dataset)  # 중복 제거로 인해 같거나 적음
        
        print(f"품질 점수: {report.metrics.quality_score:.2f}")
        print(f"품질 상태: {report.metrics.quality_status.value}")
        print(f"오류율: {report.metrics.error_rate:.1%}")
    
    @pytest.mark.asyncio
    async def test_validation_only(self, sample_dataset):
        """검증만 수행하는 테스트"""
        
        config = QualityConfig(action=QualityAction.VALIDATE_ONLY)
        engine = DataQualityEngine(config)
        
        report = await engine.process_dataset(sample_dataset, "validation_test")
        
        # 검증 결과 확인
        assert len(report.validation_results) > 0
        assert report.duplicate_result is None  # 중복 감지 안 함
        assert len(report.cleaning_results) == 0  # 정리 안 함
        
        # 원본 데이터 유지 확인
        assert len(report.processed_dataset) == len(sample_dataset)
    
    @pytest.mark.asyncio
    async def test_clean_and_validate(self, sample_dataset):
        """정리 후 검증 테스트"""
        
        config = QualityConfig(action=QualityAction.CLEAN_AND_VALIDATE)
        engine = DataQualityEngine(config)
        
        report = await engine.process_dataset(sample_dataset, "clean_test")
        
        # 정리 결과 확인
        assert len(report.cleaning_results) > 0
        assert report.metrics.auto_fixes > 0
        
        # 검증 결과 확인
        assert len(report.validation_results) > 0
        
        # 중복 제거는 안 함
        assert report.duplicate_result is None
    
    @pytest.mark.asyncio
    async def test_duplicate_removal(self, sample_dataset):
        """중복 제거 테스트"""
        
        config = QualityConfig(
            action=QualityAction.REMOVE_DUPLICATES,
            duplicate_config=DuplicateConfig(
                strategy=DuplicateStrategy.KEEP_FIRST,
                key_fields=["name", "latitude", "longitude"]
            )
        )
        engine = DataQualityEngine(config)
        
        report = await engine.process_dataset(sample_dataset, "duplicate_test")
        
        # 중복 감지 결과 확인
        assert report.duplicate_result is not None
        assert report.metrics.duplicate_groups > 0
        assert report.metrics.duplicate_records > 0
        
        # 중복 제거된 데이터 확인
        assert len(report.processed_dataset) < len(sample_dataset)
    
    def test_quality_score_calculation(self, quality_config):
        """품질 점수 계산 테스트"""
        
        engine = DataQualityEngine(quality_config)
        
        # 테스트용 메트릭스
        from app.quality.quality_engine import QualityMetrics
        
        # 좋은 품질의 메트릭스
        good_metrics = QualityMetrics(
            total_records=100,
            error_rate=0.01,  # 1% 오류율
            completeness=0.95,  # 95% 완성도
            duplicate_rate=0.02,  # 2% 중복률
            clean_success_rate=0.98  # 98% 정리 성공률
        )
        
        good_score = engine._calculate_quality_score(good_metrics)
        assert good_score > 0.8
        
        # 나쁜 품질의 메트릭스
        poor_metrics = QualityMetrics(
            total_records=100,
            error_rate=0.2,  # 20% 오류율
            completeness=0.6,  # 60% 완성도
            duplicate_rate=0.15,  # 15% 중복률
            clean_success_rate=0.5  # 50% 정리 성공률
        )
        
        poor_score = engine._calculate_quality_score(poor_metrics)
        assert poor_score < 0.6
        assert good_score > poor_score
    
    def test_recommendations_generation(self, sample_dataset, quality_config):
        """권장사항 생성 테스트"""
        
        engine = DataQualityEngine(quality_config)
        
        # 테스트용 보고서 생성
        from app.quality.quality_engine import QualityReport, QualityMetrics
        
        report = QualityReport(
            report_id="test_report",
            timestamp=datetime.now(),
            dataset_name="test_dataset",
            config=quality_config,
            metrics=QualityMetrics(
                total_records=100,
                error_rate=0.15,  # 높은 오류율
                completeness=0.7,  # 낮은 완성도
                duplicate_rate=0.08,  # 높은 중복률
                critical_errors=5  # 치명적 오류
            )
        )
        
        recommendations = engine._generate_recommendations(report)
        
        assert len(recommendations) > 0
        assert any("오류율" in rec for rec in recommendations)
        assert any("완성도" in rec for rec in recommendations)
        assert any("중복" in rec for rec in recommendations)
        assert any("치명적" in rec for rec in recommendations)


class TestDataValidator:
    """데이터 검증기 테스트"""
    
    @pytest.fixture
    def validator(self) -> DataValidator:
        """테스트용 검증기"""
        return DataValidator()
    
    def test_email_validation(self, validator):
        """이메일 검증 테스트"""
        
        # 유효한 이메일
        valid_results = validator.validate_field("email", "test@example.com")
        assert all(r.is_valid for r in valid_results if r.rule_type == "email_format")
        
        # 무효한 이메일
        invalid_results = validator.validate_field("email", "invalid-email")
        email_results = [r for r in invalid_results if r.rule_type == "email_format"]
        assert len(email_results) > 0
        assert not email_results[0].is_valid
    
    def test_coordinate_validation(self, validator):
        """좌표 검증 테스트"""
        
        # 유효한 위도
        lat_results = validator.validate_field("latitude", 37.5665)
        assert all(r.is_valid for r in lat_results if "latitude" in r.rule_type)
        
        # 범위 벗어난 위도
        invalid_lat_results = validator.validate_field("latitude", 100.0)
        lat_errors = [r for r in invalid_lat_results if r.rule_type == "latitude_range"]
        assert len(lat_errors) > 0
        assert not lat_errors[0].is_valid
    
    def test_custom_rule_addition(self, validator):
        """커스텀 규칙 추가 테스트"""
        
        # 커스텀 규칙 추가
        validator.add_custom_rule(
            field_name="rating",
            rule_name="rating_range",
            validator_func=lambda value: 1 <= float(value) <= 5,
            severity=ValidationSeverity.ERROR,
            error_message="평점은 1-5 범위여야 합니다."
        )
        
        # 규칙 적용 테스트
        valid_results = validator.validate_field("rating", 4.5)
        invalid_results = validator.validate_field("rating", 10.0)
        
        rating_errors = [r for r in invalid_results if r.rule_type == "rating_range"]
        assert len(rating_errors) > 0
        assert not rating_errors[0].is_valid


class TestDuplicateDetector:
    """중복 감지기 테스트"""
    
    @pytest.fixture
    def sample_data_with_duplicates(self) -> List[Dict[str, Any]]:
        """중복이 있는 테스트 데이터"""
        return [
            {"id": 1, "name": "경복궁", "lat": 37.579617, "lng": 126.977041},
            {"id": 2, "name": "창덕궁", "lat": 37.582219, "lng": 126.991156},
            {"id": 3, "name": "경복궁", "lat": 37.579617, "lng": 126.977041},  # 완전 중복
            {"id": 4, "name": "경복궁", "lat": 37.579620, "lng": 126.977040},  # 유사 중복
            {"id": 5, "name": "덕수궁", "lat": 37.565872, "lng": 126.975020}
        ]
    
    def test_exact_duplicate_detection(self, sample_data_with_duplicates):
        """완전 중복 감지 테스트"""
        
        config = DuplicateConfig(
            strategy=DuplicateStrategy.KEEP_FIRST,
            duplicate_type=DuplicateStrategy.EXACT
        )
        
        detector = DuplicateDetector(config)
        result = detector.detect_duplicates(sample_data_with_duplicates)
        
        assert len(result.duplicate_groups) > 0
        assert result.total_duplicates > 0
        assert len(result.unique_records) < len(sample_data_with_duplicates)
    
    def test_field_based_duplicate_detection(self, sample_data_with_duplicates):
        """필드 기반 중복 감지 테스트"""
        
        config = DuplicateConfig(
            strategy=DuplicateStrategy.KEEP_FIRST,
            key_fields=["name"]
        )
        
        detector = DuplicateDetector(config)
        result = detector.detect_duplicates(sample_data_with_duplicates)
        
        # 같은 이름의 레코드들이 중복으로 감지되어야 함
        assert len(result.duplicate_groups) > 0
        
        # "경복궁"이라는 이름을 가진 레코드들이 중복 그룹을 형성해야 함
        kyeongbok_group = next(
            (group for group in result.duplicate_groups 
             if group.key_values.get("name") == "경복궁"), 
            None
        )
        assert kyeongbok_group is not None
        assert len(kyeongbok_group.records) >= 2
    
    def test_duplicate_strategies(self, sample_data_with_duplicates):
        """중복 처리 전략 테스트"""
        
        # KEEP_FIRST 전략
        config_first = DuplicateConfig(
            strategy=DuplicateStrategy.KEEP_FIRST,
            key_fields=["name"]
        )
        detector_first = DuplicateDetector(config_first)
        result_first = detector_first.detect_duplicates(sample_data_with_duplicates)
        
        # KEEP_LAST 전략
        config_last = DuplicateConfig(
            strategy=DuplicateStrategy.KEEP_LAST,
            key_fields=["name"]
        )
        detector_last = DuplicateDetector(config_last)
        result_last = detector_last.detect_duplicates(sample_data_with_duplicates)
        
        # 둘 다 같은 수의 유니크 레코드를 가져야 함
        assert len(result_first.unique_records) == len(result_last.unique_records)
        
        # 하지만 유지되는 레코드는 다를 수 있음
        first_ids = {r["id"] for r in result_first.unique_records}
        last_ids = {r["id"] for r in result_last.unique_records}
        # 완전히 같지 않을 수도 있음 (중복 그룹에 따라)


class TestDataCleaner:
    """데이터 정리기 테스트"""
    
    @pytest.fixture
    def cleaner(self) -> DataCleaner:
        """테스트용 데이터 정리기"""
        return DataCleaner()
    
    def test_phone_number_cleaning(self, cleaner):
        """전화번호 정리 테스트"""
        
        test_data = {"phone": "010 1234 5678"}
        result = cleaner.clean_record(test_data)
        
        # 전화번호가 정규화되었는지 확인
        cleaned_phone = result.cleaned_record["phone"]
        assert "-" in cleaned_phone
        assert " " not in cleaned_phone
    
    def test_email_cleaning(self, cleaner):
        """이메일 정리 테스트"""
        
        test_data = {"email": " TEST@EXAMPLE.COM "}
        result = cleaner.clean_record(test_data)
        
        # 이메일이 소문자로 변환되고 공백이 제거되었는지 확인
        cleaned_email = result.cleaned_record["email"]
        assert cleaned_email == "test@example.com"
    
    def test_html_tag_removal(self, cleaner):
        """HTML 태그 제거 테스트"""
        
        test_data = {"description": "<p>Hello <b>world</b></p>"}
        result = cleaner.clean_record(test_data)
        
        # HTML 태그가 제거되었는지 확인
        cleaned_desc = result.cleaned_record["description"]
        assert "<" not in cleaned_desc
        assert ">" not in cleaned_desc
        assert "Hello world" in cleaned_desc
    
    def test_cleaning_summary(self, cleaner):
        """정리 요약 테스트"""
        
        test_dataset = [
            {"name": " test ", "phone": "010 1234 5678", "email": " TEST@EXAMPLE.COM "},
            {"name": "example", "phone": "02)123-4567", "email": "test@test.com"},
        ]
        
        results = cleaner.clean_dataset(test_dataset)
        summary = cleaner.get_cleaning_summary(results)
        
        assert summary["total_records"] == 2
        assert summary["total_success"] > 0
        assert summary["success_rate"] > 0
        assert "rule_statistics" in summary


if __name__ == "__main__":
    # 테스트 실행
    pytest.main([__file__, "-v"])