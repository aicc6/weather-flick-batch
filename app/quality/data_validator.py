"""
데이터 검증 엔진

입력 데이터의 무결성, 형식, 필수 필드 등을 검증합니다.
"""

import re
import logging
from datetime import datetime, date
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
import json


class ValidationSeverity(Enum):
    """검증 심각도"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class FieldType(Enum):
    """필드 타입"""
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATE = "date"
    DATETIME = "datetime"
    EMAIL = "email"
    URL = "url"
    PHONE = "phone"
    COORDINATES = "coordinates"
    JSON = "json"


@dataclass
class ValidationRule:
    """검증 규칙"""
    field_name: str
    rule_type: str
    severity: ValidationSeverity
    description: str
    validator: Callable[[Any], bool]
    error_message: str
    fix_suggestion: Optional[str] = None
    auto_fix: Optional[Callable[[Any], Any]] = None


@dataclass
class ValidationResult:
    """검증 결과"""
    field_name: str
    rule_type: str
    severity: ValidationSeverity
    is_valid: bool
    error_message: str
    original_value: Any
    suggested_fix: Optional[str] = None
    auto_fixed_value: Optional[Any] = None
    record_id: Optional[str] = None


@dataclass
class ValidationSummary:
    """검증 요약"""
    total_records: int
    valid_records: int
    invalid_records: int
    total_errors: int
    errors_by_severity: Dict[ValidationSeverity, int] = field(default_factory=dict)
    errors_by_field: Dict[str, int] = field(default_factory=dict)
    errors_by_rule: Dict[str, int] = field(default_factory=dict)


class DataValidator:
    """데이터 검증기"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.rules: Dict[str, List[ValidationRule]] = {}
        self.field_types: Dict[str, FieldType] = {}
        self._setup_default_rules()
    
    def _setup_default_rules(self):
        """기본 검증 규칙 설정"""
        
        # 필수 필드 규칙
        self.add_rule(ValidationRule(
            field_name="*",
            rule_type="required",
            severity=ValidationSeverity.ERROR,
            description="필수 필드 확인",
            validator=lambda value: value is not None and str(value).strip() != "",
            error_message="필수 필드가 비어있습니다.",
            fix_suggestion="값을 입력하세요."
        ))
        
        # 문자열 길이 규칙
        self.add_rule(ValidationRule(
            field_name="*",
            rule_type="string_length",
            severity=ValidationSeverity.WARNING,
            description="문자열 길이 확인",
            validator=lambda value: len(str(value)) <= 1000,
            error_message="문자열이 너무 깁니다 (1000자 초과).",
            fix_suggestion="문자열을 1000자 이하로 줄이세요.",
            auto_fix=lambda value: str(value)[:1000] if value else value
        ))
        
        # 이메일 형식 규칙
        email_pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
        self.add_rule(ValidationRule(
            field_name="email",
            rule_type="email_format",
            severity=ValidationSeverity.ERROR,
            description="이메일 형식 확인",
            validator=lambda value: bool(email_pattern.match(str(value))) if value else True,
            error_message="올바르지 않은 이메일 형식입니다.",
            fix_suggestion="올바른 이메일 형식으로 입력하세요."
        ))
        
        # URL 형식 규칙
        url_pattern = re.compile(r'^https?://[^\s/$.?#].[^\s]*$')
        self.add_rule(ValidationRule(
            field_name="*url*",
            rule_type="url_format",
            severity=ValidationSeverity.WARNING,
            description="URL 형식 확인",
            validator=lambda value: bool(url_pattern.match(str(value))) if value else True,
            error_message="올바르지 않은 URL 형식입니다.",
            fix_suggestion="http:// 또는 https://로 시작하는 올바른 URL을 입력하세요."
        ))
        
        # 좌표 형식 규칙 (위도/경도)
        self.add_rule(ValidationRule(
            field_name="latitude",
            rule_type="latitude_range",
            severity=ValidationSeverity.ERROR,
            description="위도 범위 확인",
            validator=lambda value: -90 <= float(value) <= 90 if value else True,
            error_message="위도는 -90 ~ 90 범위여야 합니다.",
            fix_suggestion="올바른 위도 값을 입력하세요."
        ))
        
        self.add_rule(ValidationRule(
            field_name="longitude",
            rule_type="longitude_range",
            severity=ValidationSeverity.ERROR,
            description="경도 범위 확인",
            validator=lambda value: -180 <= float(value) <= 180 if value else True,
            error_message="경도는 -180 ~ 180 범위여야 합니다.",
            fix_suggestion="올바른 경도 값을 입력하세요."
        ))
        
        # 한국 좌표 범위 (더 구체적)
        self.add_rule(ValidationRule(
            field_name="latitude",
            rule_type="korea_latitude",
            severity=ValidationSeverity.WARNING,
            description="한국 위도 범위 확인",
            validator=lambda value: 33 <= float(value) <= 39 if value else True,
            error_message="한국 위도 범위(33-39)를 벗어났습니다.",
            fix_suggestion="한국 지역의 위도인지 확인하세요."
        ))
        
        self.add_rule(ValidationRule(
            field_name="longitude",
            rule_type="korea_longitude",
            severity=ValidationSeverity.WARNING,
            description="한국 경도 범위 확인",
            validator=lambda value: 124 <= float(value) <= 132 if value else True,
            error_message="한국 경도 범위(124-132)를 벗어났습니다.",
            fix_suggestion="한국 지역의 경도인지 확인하세요."
        ))
        
        # 전화번호 형식 규칙
        phone_pattern = re.compile(r'^(\+82|0)?\d{2,3}-?\d{3,4}-?\d{4}$')
        self.add_rule(ValidationRule(
            field_name="*phone*",
            rule_type="phone_format",
            severity=ValidationSeverity.WARNING,
            description="전화번호 형식 확인",
            validator=lambda value: bool(phone_pattern.match(str(value).replace(' ', ''))) if value else True,
            error_message="올바르지 않은 전화번호 형식입니다.",
            fix_suggestion="010-1234-5678 형식으로 입력하세요.",
            auto_fix=lambda value: self._normalize_phone(value) if value else value
        ))
        
        # 날짜 형식 규칙
        self.add_rule(ValidationRule(
            field_name="*date*",
            rule_type="date_format",
            severity=ValidationSeverity.ERROR,
            description="날짜 형식 확인",
            validator=lambda value: self._is_valid_date(value),
            error_message="올바르지 않은 날짜 형식입니다.",
            fix_suggestion="YYYY-MM-DD 형식으로 입력하세요."
        ))
        
        # JSON 형식 규칙
        self.add_rule(ValidationRule(
            field_name="*json*",
            rule_type="json_format",
            severity=ValidationSeverity.ERROR,
            description="JSON 형식 확인",
            validator=lambda value: self._is_valid_json(value),
            error_message="올바르지 않은 JSON 형식입니다.",
            fix_suggestion="올바른 JSON 형식으로 입력하세요."
        ))
    
    def add_rule(self, rule: ValidationRule):
        """검증 규칙 추가"""
        if rule.field_name not in self.rules:
            self.rules[rule.field_name] = []
        self.rules[rule.field_name].append(rule)
        self.logger.debug(f"검증 규칙 추가: {rule.field_name} - {rule.rule_type}")
    
    def add_field_type(self, field_name: str, field_type: FieldType):
        """필드 타입 정의 추가"""
        self.field_types[field_name] = field_type
        self.logger.debug(f"필드 타입 정의: {field_name} -> {field_type.value}")
    
    def validate_record(self, record: Dict[str, Any], record_id: Optional[str] = None) -> List[ValidationResult]:
        """단일 레코드 검증"""
        results = []
        
        try:
            for field_name, value in record.items():
                field_results = self.validate_field(field_name, value, record_id)
                results.extend(field_results)
        except Exception as e:
            self.logger.error(f"레코드 검증 오류: {e}")
            results.append(ValidationResult(
                field_name="record",
                rule_type="validation_error",
                severity=ValidationSeverity.CRITICAL,
                is_valid=False,
                error_message=f"검증 중 오류 발생: {e}",
                original_value=record,
                record_id=record_id
            ))
        
        return results
    
    def validate_field(self, field_name: str, value: Any, record_id: Optional[str] = None) -> List[ValidationResult]:
        """단일 필드 검증"""
        results = []
        
        # 해당 필드에 적용할 규칙들 수집
        applicable_rules = []
        
        # 정확한 필드명 매칭
        if field_name in self.rules:
            applicable_rules.extend(self.rules[field_name])
        
        # 와일드카드 매칭
        for rule_field, rules in self.rules.items():
            if rule_field == "*":
                applicable_rules.extend(rules)
            elif "*" in rule_field:
                pattern = rule_field.replace("*", ".*")
                if re.match(pattern, field_name, re.IGNORECASE):
                    applicable_rules.extend(rules)
        
        # 각 규칙 적용
        for rule in applicable_rules:
            try:
                is_valid = rule.validator(value)
                
                if not is_valid:
                    # 자동 수정 시도
                    auto_fixed_value = None
                    if rule.auto_fix:
                        try:
                            auto_fixed_value = rule.auto_fix(value)
                        except Exception as e:
                            self.logger.warning(f"자동 수정 실패 ({field_name}): {e}")
                    
                    result = ValidationResult(
                        field_name=field_name,
                        rule_type=rule.rule_type,
                        severity=rule.severity,
                        is_valid=False,
                        error_message=rule.error_message,
                        original_value=value,
                        suggested_fix=rule.fix_suggestion,
                        auto_fixed_value=auto_fixed_value,
                        record_id=record_id
                    )
                    results.append(result)
                
            except Exception as e:
                self.logger.error(f"규칙 검증 오류 ({field_name}, {rule.rule_type}): {e}")
                results.append(ValidationResult(
                    field_name=field_name,
                    rule_type=rule.rule_type,
                    severity=ValidationSeverity.CRITICAL,
                    is_valid=False,
                    error_message=f"검증 규칙 실행 오류: {e}",
                    original_value=value,
                    record_id=record_id
                ))
        
        return results
    
    def validate_dataset(self, dataset: List[Dict[str, Any]], id_field: str = "id") -> tuple[List[ValidationResult], ValidationSummary]:
        """데이터셋 전체 검증"""
        all_results = []
        valid_records = 0
        
        self.logger.info(f"데이터셋 검증 시작: {len(dataset)}개 레코드")
        
        for i, record in enumerate(dataset):
            record_id = record.get(id_field, f"record_{i}")
            record_results = self.validate_record(record, str(record_id))
            
            if not record_results or all(r.is_valid for r in record_results):
                valid_records += 1
            
            all_results.extend(record_results)
        
        # 요약 통계 생성
        summary = self._create_summary(dataset, all_results, valid_records)
        
        self.logger.info(
            f"데이터셋 검증 완료: {valid_records}/{len(dataset)} 유효, "
            f"{len(all_results)} 오류"
        )
        
        return all_results, summary
    
    def _create_summary(self, dataset: List[Dict[str, Any]], results: List[ValidationResult], valid_records: int) -> ValidationSummary:
        """검증 요약 생성"""
        
        errors_by_severity = {severity: 0 for severity in ValidationSeverity}
        errors_by_field = {}
        errors_by_rule = {}
        
        for result in results:
            if not result.is_valid:
                # 심각도별 집계
                errors_by_severity[result.severity] += 1
                
                # 필드별 집계
                if result.field_name not in errors_by_field:
                    errors_by_field[result.field_name] = 0
                errors_by_field[result.field_name] += 1
                
                # 규칙별 집계
                if result.rule_type not in errors_by_rule:
                    errors_by_rule[result.rule_type] = 0
                errors_by_rule[result.rule_type] += 1
        
        return ValidationSummary(
            total_records=len(dataset),
            valid_records=valid_records,
            invalid_records=len(dataset) - valid_records,
            total_errors=len([r for r in results if not r.is_valid]),
            errors_by_severity=errors_by_severity,
            errors_by_field=errors_by_field,
            errors_by_rule=errors_by_rule
        )
    
    def _normalize_phone(self, phone: str) -> str:
        """전화번호 정규화"""
        if not phone:
            return phone
        
        # 숫자만 추출
        digits = re.sub(r'\D', '', str(phone))
        
        # 한국 번호 형식으로 변환
        if digits.startswith('82'):
            digits = '0' + digits[2:]
        
        if len(digits) == 11 and digits.startswith('010'):
            return f"{digits[:3]}-{digits[3:7]}-{digits[7:]}"
        elif len(digits) == 10:
            return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
        
        return phone
    
    def _is_valid_date(self, value: Any) -> bool:
        """날짜 유효성 확인"""
        if not value:
            return True
        
        try:
            if isinstance(value, (date, datetime)):
                return True
            
            # 문자열 날짜 파싱 시도
            date_str = str(value)
            
            # 다양한 날짜 형식 시도
            date_formats = [
                '%Y-%m-%d',
                '%Y/%m/%d',
                '%Y.%m.%d',
                '%Y-%m-%d %H:%M:%S',
                '%Y/%m/%d %H:%M:%S'
            ]
            
            for date_format in date_formats:
                try:
                    datetime.strptime(date_str, date_format)
                    return True
                except ValueError:
                    continue
            
            return False
            
        except Exception:
            return False
    
    def _is_valid_json(self, value: Any) -> bool:
        """JSON 유효성 확인"""
        if not value:
            return True
        
        try:
            if isinstance(value, (dict, list)):
                return True
            
            json.loads(str(value))
            return True
        except (json.JSONDecodeError, TypeError):
            return False
    
    def get_field_specific_rules(self, field_name: str) -> List[ValidationRule]:
        """특정 필드에 적용되는 규칙들 반환"""
        applicable_rules = []
        
        # 정확한 필드명 매칭
        if field_name in self.rules:
            applicable_rules.extend(self.rules[field_name])
        
        # 와일드카드 매칭
        for rule_field, rules in self.rules.items():
            if rule_field == "*":
                applicable_rules.extend(rules)
            elif "*" in rule_field:
                pattern = rule_field.replace("*", ".*")
                if re.match(pattern, field_name, re.IGNORECASE):
                    applicable_rules.extend(rules)
        
        return applicable_rules
    
    def add_custom_rule(self, field_name: str, rule_name: str, validator_func: Callable, **kwargs):
        """커스텀 검증 규칙 추가"""
        rule = ValidationRule(
            field_name=field_name,
            rule_type=rule_name,
            severity=kwargs.get('severity', ValidationSeverity.WARNING),
            description=kwargs.get('description', f"{field_name} 커스텀 검증"),
            validator=validator_func,
            error_message=kwargs.get('error_message', f"{field_name} 검증 실패"),
            fix_suggestion=kwargs.get('fix_suggestion'),
            auto_fix=kwargs.get('auto_fix')
        )
        self.add_rule(rule)


# 싱글톤 인스턴스
_data_validator = None


def get_data_validator() -> DataValidator:
    """데이터 검증기 인스턴스 반환"""
    global _data_validator
    if _data_validator is None:
        _data_validator = DataValidator()
    return _data_validator


def reset_data_validator():
    """데이터 검증기 리셋"""
    global _data_validator
    _data_validator = None