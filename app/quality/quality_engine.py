"""
데이터 품질 엔진

검증, 중복 감지, 데이터 정리를 통합하여 종합적인 데이터 품질 관리를 제공합니다.
"""

import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
import json

from .data_validator import DataValidator, ValidationResult, ValidationSeverity
from .duplicate_detector import DuplicateDetector, DuplicateConfig, DuplicateResult, DuplicateStrategy
from .data_cleaner import DataCleaner, AutoFixResult


class QualityAction(Enum):
    """품질 처리 액션"""
    VALIDATE_ONLY = "validate_only"      # 검증만 수행
    CLEAN_AND_VALIDATE = "clean_validate"  # 정리 후 검증
    REMOVE_DUPLICATES = "remove_duplicates"  # 중복 제거
    FULL_PROCESSING = "full_processing"   # 전체 처리


class QualityStatus(Enum):
    """품질 상태"""
    EXCELLENT = "excellent"    # 우수 (오류 없음)
    GOOD = "good"             # 양호 (경미한 오류)
    FAIR = "fair"             # 보통 (중간 수준 오류)
    POOR = "poor"             # 불량 (심각한 오류)
    CRITICAL = "critical"     # 치명적 (시스템 오류)


@dataclass
class QualityConfig:
    """품질 엔진 설정"""
    
    # 처리 액션
    action: QualityAction = QualityAction.FULL_PROCESSING
    
    # 품질 기준
    max_error_rate: float = 0.05        # 최대 오류율 (5%)
    max_warning_rate: float = 0.20      # 최대 경고율 (20%)
    min_completeness: float = 0.95      # 최소 완성도 (95%)
    
    # 중복 감지 설정
    duplicate_config: DuplicateConfig = field(default_factory=lambda: DuplicateConfig(
        strategy=DuplicateStrategy.KEEP_FIRST
    ))
    
    # 자동 수정 설정
    auto_fix_enabled: bool = True
    auto_fix_threshold: ValidationSeverity = ValidationSeverity.WARNING
    
    # 필수 필드 정의
    required_fields: List[str] = field(default_factory=list)
    
    # 품질 임계값
    quality_thresholds: Dict[str, float] = field(default_factory=lambda: {
        "excellent": 0.98,
        "good": 0.90,
        "fair": 0.75,
        "poor": 0.50
    })


@dataclass 
class QualityMetrics:
    """품질 지표"""
    
    # 기본 통계
    total_records: int = 0
    processed_records: int = 0
    valid_records: int = 0
    
    # 오류 통계
    total_errors: int = 0
    critical_errors: int = 0
    error_rate: float = 0.0
    warning_rate: float = 0.0
    
    # 중복 통계
    duplicate_groups: int = 0
    duplicate_records: int = 0
    duplicate_rate: float = 0.0
    
    # 정리 통계
    cleaned_fields: int = 0
    auto_fixes: int = 0
    clean_success_rate: float = 0.0
    
    # 완성도 통계
    completeness: float = 0.0
    field_completeness: Dict[str, float] = field(default_factory=dict)
    
    # 품질 점수
    quality_score: float = 0.0
    quality_status: QualityStatus = QualityStatus.POOR


@dataclass
class QualityReport:
    """품질 보고서"""
    
    # 메타 정보
    report_id: str
    timestamp: datetime
    dataset_name: str
    config: QualityConfig
    
    # 지표
    metrics: QualityMetrics
    
    # 상세 결과
    validation_results: List[ValidationResult] = field(default_factory=list)
    duplicate_result: Optional[DuplicateResult] = None
    cleaning_results: List[AutoFixResult] = field(default_factory=list)
    
    # 정리된 데이터
    original_dataset: List[Dict[str, Any]] = field(default_factory=list)
    processed_dataset: List[Dict[str, Any]] = field(default_factory=list)
    
    # 오류 요약
    error_summary: Dict[str, Any] = field(default_factory=dict)
    recommendations: List[str] = field(default_factory=list)


class DataQualityEngine:
    """데이터 품질 엔진"""
    
    def __init__(self, config: QualityConfig = None):
        self.config = config or QualityConfig()
        self.logger = logging.getLogger(__name__)
        
        # 컴포넌트 초기화
        self.validator = DataValidator()
        self.duplicate_detector = DuplicateDetector(self.config.duplicate_config)
        self.cleaner = DataCleaner()
        
        # 필수 필드 검증 규칙 추가
        self._setup_required_field_rules()
    
    def _setup_required_field_rules(self):
        """필수 필드 검증 규칙 설정"""
        for field in self.config.required_fields:
            self.validator.add_custom_rule(
                field_name=field,
                rule_name=f"required_{field}",
                validator_func=lambda value: value is not None and str(value).strip() != "",
                severity=ValidationSeverity.CRITICAL,
                description=f"필수 필드 {field} 검증",
                error_message=f"필수 필드 {field}가 비어있습니다."
            )
    
    async def process_dataset(self, 
                            dataset: List[Dict[str, Any]], 
                            dataset_name: str = "dataset",
                            id_field: str = "id") -> QualityReport:
        """데이터셋 품질 처리"""
        
        report_id = f"quality_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.logger.info(f"데이터 품질 처리 시작: {dataset_name} ({len(dataset)}개 레코드)")
        
        # 보고서 초기화
        report = QualityReport(
            report_id=report_id,
            timestamp=datetime.now(),
            dataset_name=dataset_name,
            config=self.config,
            metrics=QualityMetrics(total_records=len(dataset)),
            original_dataset=dataset.copy()
        )
        
        try:
            # 처리 단계별 실행
            processed_data = dataset.copy()
            
            if self.config.action in [QualityAction.CLEAN_AND_VALIDATE, QualityAction.FULL_PROCESSING]:
                processed_data, report = await self._clean_data(processed_data, report)
            
            if self.config.action in [QualityAction.REMOVE_DUPLICATES, QualityAction.FULL_PROCESSING]:
                processed_data, report = await self._remove_duplicates(processed_data, report)
            
            # 검증은 모든 액션에서 수행
            processed_data, report = await self._validate_data(processed_data, report, id_field)
            
            # 최종 품질 분석
            report = await self._analyze_quality(processed_data, report)
            
            # 처리된 데이터 설정
            report.processed_dataset = processed_data
            
            self.logger.info(
                f"데이터 품질 처리 완료: {dataset_name} "
                f"(품질점수: {report.metrics.quality_score:.2f}, "
                f"상태: {report.metrics.quality_status.value})"
            )
            
        except Exception as e:
            self.logger.error(f"데이터 품질 처리 오류: {e}")
            report.error_summary["processing_error"] = str(e)
            report.metrics.quality_status = QualityStatus.CRITICAL
        
        return report
    
    async def _clean_data(self, dataset: List[Dict[str, Any]], report: QualityReport) -> Tuple[List[Dict[str, Any]], QualityReport]:
        """데이터 정리"""
        
        self.logger.info("데이터 정리 시작")
        
        try:
            # 데이터 정리 실행
            cleaning_results = self.cleaner.clean_dataset(dataset)
            
            # 정리된 데이터 수집
            cleaned_dataset = self.cleaner.get_cleaned_dataset(cleaning_results)
            
            # 정리 통계 업데이트
            report.cleaning_results = cleaning_results
            report.metrics.cleaned_fields = sum(len(r.cleaning_results) for r in cleaning_results)
            report.metrics.auto_fixes = sum(r.success_count for r in cleaning_results)
            
            total_cleanings = sum(r.success_count + r.error_count for r in cleaning_results)
            if total_cleanings > 0:
                report.metrics.clean_success_rate = report.metrics.auto_fixes / total_cleanings
            
            self.logger.info(f"데이터 정리 완료: {report.metrics.auto_fixes}개 자동 수정")
            
            return cleaned_dataset, report
            
        except Exception as e:
            self.logger.error(f"데이터 정리 오류: {e}")
            report.error_summary["cleaning_error"] = str(e)
            return dataset, report
    
    async def _remove_duplicates(self, dataset: List[Dict[str, Any]], report: QualityReport) -> Tuple[List[Dict[str, Any]], QualityReport]:
        """중복 제거"""
        
        self.logger.info("중복 데이터 감지 및 제거 시작")
        
        try:
            # 중복 감지 및 제거
            duplicate_result = self.duplicate_detector.detect_duplicates(dataset)
            
            # 중복 통계 업데이트
            report.duplicate_result = duplicate_result
            report.metrics.duplicate_groups = len(duplicate_result.duplicate_groups)
            report.metrics.duplicate_records = duplicate_result.total_duplicates
            if len(dataset) > 0:
                report.metrics.duplicate_rate = duplicate_result.total_duplicates / len(dataset)
            
            self.logger.info(
                f"중복 제거 완료: {duplicate_result.total_duplicates}개 중복 제거"
            )
            
            return duplicate_result.unique_records, report
            
        except Exception as e:
            self.logger.error(f"중복 제거 오류: {e}")
            report.error_summary["duplicate_error"] = str(e)
            return dataset, report
    
    async def _validate_data(self, dataset: List[Dict[str, Any]], report: QualityReport, id_field: str) -> Tuple[List[Dict[str, Any]], QualityReport]:
        """데이터 검증"""
        
        self.logger.info("데이터 검증 시작")
        
        try:
            # 데이터 검증 실행
            validation_results, validation_summary = self.validator.validate_dataset(dataset, id_field)
            
            # 검증 통계 업데이트
            report.validation_results = validation_results
            report.metrics.processed_records = len(dataset)
            report.metrics.valid_records = validation_summary.valid_records
            report.metrics.total_errors = validation_summary.total_errors
            
            # 오류율 계산
            if len(dataset) > 0:
                report.metrics.error_rate = validation_summary.total_errors / len(dataset)
                
                # 경고율 계산 (WARNING 이상)
                warnings = validation_summary.errors_by_severity.get(ValidationSeverity.WARNING, 0)
                errors = validation_summary.errors_by_severity.get(ValidationSeverity.ERROR, 0)
                critical = validation_summary.errors_by_severity.get(ValidationSeverity.CRITICAL, 0)
                report.metrics.warning_rate = (warnings + errors + critical) / len(dataset)
            
            # 치명적 오류 수
            report.metrics.critical_errors = validation_summary.errors_by_severity.get(ValidationSeverity.CRITICAL, 0)
            
            self.logger.info(
                f"데이터 검증 완료: {validation_summary.valid_records}/{len(dataset)} 유효, "
                f"{validation_summary.total_errors}개 오류"
            )
            
            return dataset, report
            
        except Exception as e:
            self.logger.error(f"데이터 검증 오류: {e}")
            report.error_summary["validation_error"] = str(e)
            return dataset, report
    
    async def _analyze_quality(self, dataset: List[Dict[str, Any]], report: QualityReport) -> QualityReport:
        """품질 분석"""
        
        try:
            # 완성도 계산
            report.metrics.completeness = self._calculate_completeness(dataset)
            report.metrics.field_completeness = self._calculate_field_completeness(dataset)
            
            # 품질 점수 계산
            report.metrics.quality_score = self._calculate_quality_score(report.metrics)
            
            # 품질 상태 결정
            report.metrics.quality_status = self._determine_quality_status(report.metrics)
            
            # 오류 요약 생성
            report.error_summary = self._create_error_summary(report)
            
            # 권장사항 생성
            report.recommendations = self._generate_recommendations(report)
            
            return report
            
        except Exception as e:
            self.logger.error(f"품질 분석 오류: {e}")
            report.error_summary["analysis_error"] = str(e)
            return report
    
    def _calculate_completeness(self, dataset: List[Dict[str, Any]]) -> float:
        """완성도 계산"""
        if not dataset:
            return 0.0
        
        total_fields = 0
        filled_fields = 0
        
        for record in dataset:
            for field, value in record.items():
                total_fields += 1
                if value is not None and str(value).strip():
                    filled_fields += 1
        
        return filled_fields / total_fields if total_fields > 0 else 0.0
    
    def _calculate_field_completeness(self, dataset: List[Dict[str, Any]]) -> Dict[str, float]:
        """필드별 완성도 계산"""
        if not dataset:
            return {}
        
        field_stats = {}
        
        # 모든 필드 수집
        all_fields = set()
        for record in dataset:
            all_fields.update(record.keys())
        
        # 필드별 완성도 계산
        for field in all_fields:
            filled_count = 0
            for record in dataset:
                value = record.get(field)
                if value is not None and str(value).strip():
                    filled_count += 1
            
            field_stats[field] = filled_count / len(dataset)
        
        return field_stats
    
    def _calculate_quality_score(self, metrics: QualityMetrics) -> float:
        """품질 점수 계산 (0-1)"""
        
        # 가중치 설정
        weights = {
            "error_rate": 0.4,      # 오류율 (낮을수록 좋음)
            "completeness": 0.3,    # 완성도 (높을수록 좋음)  
            "duplicate_rate": 0.2,  # 중복률 (낮을수록 좋음)
            "clean_rate": 0.1       # 정리 성공률 (높을수록 좋음)
        }
        
        # 각 지표 점수 계산 (0-1)
        error_score = max(0, 1 - metrics.error_rate / 0.1)  # 10% 오류율을 기준
        completeness_score = metrics.completeness
        duplicate_score = max(0, 1 - metrics.duplicate_rate / 0.1)  # 10% 중복률을 기준
        clean_score = metrics.clean_success_rate
        
        # 가중 평균 계산
        quality_score = (
            error_score * weights["error_rate"] +
            completeness_score * weights["completeness"] +
            duplicate_score * weights["duplicate_rate"] +
            clean_score * weights["clean_rate"]
        )
        
        return min(1.0, max(0.0, quality_score))
    
    def _determine_quality_status(self, metrics: QualityMetrics) -> QualityStatus:
        """품질 상태 결정"""
        
        # 치명적 오류가 있으면 CRITICAL
        if metrics.critical_errors > 0:
            return QualityStatus.CRITICAL
        
        # 품질 점수 기반 분류
        score = metrics.quality_score
        thresholds = self.config.quality_thresholds
        
        if score >= thresholds["excellent"]:
            return QualityStatus.EXCELLENT
        elif score >= thresholds["good"]:
            return QualityStatus.GOOD
        elif score >= thresholds["fair"]:
            return QualityStatus.FAIR
        else:
            return QualityStatus.POOR
    
    def _create_error_summary(self, report: QualityReport) -> Dict[str, Any]:
        """오류 요약 생성"""
        
        summary = {
            "total_errors": report.metrics.total_errors,
            "error_rate": report.metrics.error_rate,
            "critical_errors": report.metrics.critical_errors
        }
        
        # 검증 오류 요약
        if report.validation_results:
            error_by_field = {}
            error_by_type = {}
            
            for result in report.validation_results:
                if not result.is_valid:
                    # 필드별 집계
                    if result.field_name not in error_by_field:
                        error_by_field[result.field_name] = 0
                    error_by_field[result.field_name] += 1
                    
                    # 타입별 집계
                    if result.rule_type not in error_by_type:
                        error_by_type[result.rule_type] = 0
                    error_by_type[result.rule_type] += 1
            
            summary["errors_by_field"] = error_by_field
            summary["errors_by_type"] = error_by_type
        
        # 중복 오류 요약
        if report.duplicate_result:
            summary["duplicates"] = {
                "groups": report.metrics.duplicate_groups,
                "records": report.metrics.duplicate_records,
                "rate": report.metrics.duplicate_rate
            }
        
        return summary
    
    def _generate_recommendations(self, report: QualityReport) -> List[str]:
        """권장사항 생성"""
        recommendations = []
        
        # 오류율 기반 권장사항
        if report.metrics.error_rate > self.config.max_error_rate:
            recommendations.append(
                f"데이터 오류율이 높습니다 ({report.metrics.error_rate:.1%}). "
                f"데이터 입력 프로세스를 점검하세요."
            )
        
        # 완성도 기반 권장사항
        if report.metrics.completeness < self.config.min_completeness:
            recommendations.append(
                f"데이터 완성도가 낮습니다 ({report.metrics.completeness:.1%}). "
                f"필수 필드 입력을 강화하세요."
            )
        
        # 중복률 기반 권장사항
        if report.metrics.duplicate_rate > 0.05:  # 5%
            recommendations.append(
                f"중복 데이터가 많습니다 ({report.metrics.duplicate_rate:.1%}). "
                f"데이터 수집 프로세스를 개선하세요."
            )
        
        # 치명적 오류 기반 권장사항
        if report.metrics.critical_errors > 0:
            recommendations.append(
                f"치명적 오류가 {report.metrics.critical_errors}개 발견되었습니다. "
                f"즉시 수정이 필요합니다."
            )
        
        # 필드별 완성도 기반 권장사항
        for field, completeness in report.metrics.field_completeness.items():
            if completeness < 0.8 and field in self.config.required_fields:
                recommendations.append(
                    f"필수 필드 '{field}'의 완성도가 낮습니다 ({completeness:.1%}). "
                    f"데이터 수집을 강화하세요."
                )
        
        return recommendations
    
    def get_quality_summary(self, report: QualityReport) -> Dict[str, Any]:
        """품질 요약 정보 반환"""
        return {
            "report_id": report.report_id,
            "dataset_name": report.dataset_name,
            "timestamp": report.timestamp.isoformat(),
            "quality_score": report.metrics.quality_score,
            "quality_status": report.metrics.quality_status.value,
            "total_records": report.metrics.total_records,
            "valid_records": report.metrics.valid_records,
            "error_rate": report.metrics.error_rate,
            "completeness": report.metrics.completeness,
            "duplicate_rate": report.metrics.duplicate_rate,
            "recommendations_count": len(report.recommendations)
        }
    
    def export_report_to_json(self, report: QualityReport, file_path: str):
        """보고서를 JSON 파일로 내보내기"""
        
        # 직렬화 가능한 형태로 변환
        report_dict = {
            "report_id": report.report_id,
            "timestamp": report.timestamp.isoformat(),
            "dataset_name": report.dataset_name,
            "config": {
                "action": report.config.action.value,
                "max_error_rate": report.config.max_error_rate,
                "max_warning_rate": report.config.max_warning_rate,
                "min_completeness": report.config.min_completeness,
                "required_fields": report.config.required_fields
            },
            "metrics": {
                "total_records": report.metrics.total_records,
                "processed_records": report.metrics.processed_records,
                "valid_records": report.metrics.valid_records,
                "total_errors": report.metrics.total_errors,
                "error_rate": report.metrics.error_rate,
                "completeness": report.metrics.completeness,
                "quality_score": report.metrics.quality_score,
                "quality_status": report.metrics.quality_status.value,
                "field_completeness": report.metrics.field_completeness
            },
            "error_summary": report.error_summary,
            "recommendations": report.recommendations
        }
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(report_dict, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"품질 보고서 저장 완료: {file_path}")


# 싱글톤 인스턴스
_quality_engine = None


def get_quality_engine(config: QualityConfig = None) -> DataQualityEngine:
    """품질 엔진 인스턴스 반환"""
    global _quality_engine
    if _quality_engine is None or config is not None:
        _quality_engine = DataQualityEngine(config)
    return _quality_engine


def reset_quality_engine():
    """품질 엔진 리셋"""
    global _quality_engine
    _quality_engine = None