"""
데이터 정리 및 자동 수정 시스템

데이터 품질 문제를 자동으로 감지하고 수정하는 기능을 제공합니다.
"""

import re
import logging
from typing import Dict, List, Any, Optional, Callable, Union
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime


class CleaningAction(Enum):
    """정리 작업 유형"""
    TRIM = "trim"                    # 공백 제거
    NORMALIZE = "normalize"          # 정규화
    REPLACE = "replace"              # 값 치환
    REMOVE = "remove"                # 값 제거
    FORMAT = "format"                # 형식 변환
    VALIDATE = "validate"            # 검증 후 수정
    EXTRACT = "extract"              # 값 추출
    MERGE = "merge"                  # 값 병합
    CALCULATE = "calculate"          # 값 계산


class CleaningSeverity(Enum):
    """정리 심각도"""
    LOW = "low"                      # 낮음 (형식 정리)
    MEDIUM = "medium"                # 중간 (데이터 변환)
    HIGH = "high"                    # 높음 (데이터 수정)
    CRITICAL = "critical"            # 치명적 (필수 수정)


@dataclass
class CleaningRule:
    """데이터 정리 규칙"""
    name: str
    field_pattern: str               # 적용할 필드 패턴
    action: CleaningAction
    severity: CleaningSeverity
    description: str
    condition: Callable[[Any], bool]  # 적용 조건
    transform: Callable[[Any], Any]   # 변환 함수
    validation: Optional[Callable[[Any], bool]] = None  # 변환 후 검증
    examples: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class CleaningResult:
    """정리 결과"""
    field_name: str
    rule_name: str
    action: CleaningAction
    severity: CleaningSeverity
    original_value: Any
    cleaned_value: Any
    success: bool
    error_message: Optional[str] = None


@dataclass
class AutoFixResult:
    """자동 수정 결과"""
    original_record: Dict[str, Any]
    cleaned_record: Dict[str, Any]
    cleaning_results: List[CleaningResult]
    success_count: int
    error_count: int
    record_id: Optional[str] = None


class DataCleaner:
    """데이터 정리기"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.rules: List[CleaningRule] = []
        self._setup_default_rules()
    
    def _setup_default_rules(self):
        """기본 정리 규칙 설정"""
        
        # 1. 공백 제거 규칙
        self.add_rule(CleaningRule(
            name="trim_whitespace",
            field_pattern="*",
            action=CleaningAction.TRIM,
            severity=CleaningSeverity.LOW,
            description="문자열 앞뒤 공백 제거",
            condition=lambda value: isinstance(value, str) and (value.startswith(' ') or value.endswith(' ')),
            transform=lambda value: str(value).strip(),
            examples=[
                {"before": " hello world ", "after": "hello world"},
                {"before": "\t\n text \r\n", "after": "text"}
            ]
        ))
        
        # 2. 전화번호 정규화
        self.add_rule(CleaningRule(
            name="normalize_phone",
            field_pattern="*phone*|*tel*",
            action=CleaningAction.NORMALIZE,
            severity=CleaningSeverity.MEDIUM,
            description="전화번호 형식 정규화",
            condition=lambda value: value and re.search(r'\d{3,4}[-\s]?\d{3,4}[-\s]?\d{4}', str(value)),
            transform=self._normalize_phone_number,
            validation=lambda value: bool(re.match(r'^\d{2,3}-\d{3,4}-\d{4}$', str(value))),
            examples=[
                {"before": "010 1234 5678", "after": "010-1234-5678"},
                {"before": "02)123-4567", "after": "02-123-4567"}
            ]
        ))
        
        # 3. 이메일 정규화
        self.add_rule(CleaningRule(
            name="normalize_email",
            field_pattern="*email*",
            action=CleaningAction.NORMALIZE,
            severity=CleaningSeverity.MEDIUM,
            description="이메일 주소 정규화",
            condition=lambda value: value and '@' in str(value),
            transform=lambda value: str(value).lower().strip(),
            validation=lambda value: bool(re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', str(value))),
            examples=[
                {"before": " USER@EXAMPLE.COM ", "after": "user@example.com"},
                {"before": "Test.Email@Domain.org", "after": "test.email@domain.org"}
            ]
        ))
        
        # 4. URL 정규화
        self.add_rule(CleaningRule(
            name="normalize_url",
            field_pattern="*url*|*link*|*website*|*homepage*",
            action=CleaningAction.NORMALIZE,
            severity=CleaningSeverity.MEDIUM,
            description="URL 정규화",
            condition=lambda value: value and ('http' in str(value) or 'www.' in str(value)),
            transform=self._normalize_url,
            validation=lambda value: bool(re.match(r'^https?://[^\s/$.?#].[^\s]*$', str(value))),
            examples=[
                {"before": "www.example.com", "after": "http://www.example.com"},
                {"before": "HTTPS://EXAMPLE.COM/", "after": "https://example.com"}
            ]
        ))
        
        # 5. 좌표값 정리
        self.add_rule(CleaningRule(
            name="clean_coordinates",
            field_pattern="*lat*|*lng*|*lon*",
            action=CleaningAction.FORMAT,
            severity=CleaningSeverity.MEDIUM,
            description="좌표값 소수점 정리",
            condition=lambda value: value and self._is_numeric(value),
            transform=lambda value: round(float(value), 6),
            validation=lambda value: isinstance(value, (int, float)),
            examples=[
                {"before": "37.123456789", "after": 37.123457},
                {"before": "127.0", "after": 127.0}
            ]
        ))
        
        # 6. 날짜 형식 정규화
        self.add_rule(CleaningRule(
            name="normalize_date",
            field_pattern="*date*|*time*",
            action=CleaningAction.FORMAT,
            severity=CleaningSeverity.HIGH,
            description="날짜 형식 정규화",
            condition=lambda value: value and self._looks_like_date(value),
            transform=self._normalize_date,
            validation=lambda value: self._is_valid_date_format(value),
            examples=[
                {"before": "2024/01/15", "after": "2024-01-15"},
                {"before": "15.01.2024", "after": "2024-01-15"}
            ]
        ))
        
        # 7. 빈 문자열을 None으로 변환
        self.add_rule(CleaningRule(
            name="empty_to_null",
            field_pattern="*",
            action=CleaningAction.REPLACE,
            severity=CleaningSeverity.LOW,
            description="빈 문자열을 None으로 변환",
            condition=lambda value: value == "" or value == "null" or value == "NULL",
            transform=lambda value: None,
            examples=[
                {"before": "", "after": None},
                {"before": "NULL", "after": None}
            ]
        ))
        
        # 8. HTML 태그 제거
        self.add_rule(CleaningRule(
            name="remove_html_tags",
            field_pattern="*desc*|*content*|*text*",
            action=CleaningAction.REMOVE,
            severity=CleaningSeverity.MEDIUM,
            description="HTML 태그 제거",
            condition=lambda value: value and '<' in str(value) and '>' in str(value),
            transform=self._remove_html_tags,
            examples=[
                {"before": "<p>Hello <b>world</b></p>", "after": "Hello world"},
                {"before": "Text with <script>alert('xss')</script>", "after": "Text with alert('xss')"}
            ]
        ))
        
        # 9. 중복 공백 정리
        self.add_rule(CleaningRule(
            name="normalize_spaces",
            field_pattern="*",
            action=CleaningAction.NORMALIZE,
            severity=CleaningSeverity.LOW,
            description="중복 공백을 단일 공백으로 변환",
            condition=lambda value: isinstance(value, str) and '  ' in value,
            transform=lambda value: re.sub(r'\s+', ' ', str(value)),
            examples=[
                {"before": "hello    world", "after": "hello world"},
                {"before": "text\t\twith\n\ntabs", "after": "text with tabs"}
            ]
        ))
        
        # 10. 숫자 문자열 정리
        self.add_rule(CleaningRule(
            name="clean_numeric_string",
            field_pattern="*price*|*amount*|*count*|*number*",
            action=CleaningAction.FORMAT,
            severity=CleaningSeverity.MEDIUM,
            description="숫자 문자열 정리",
            condition=lambda value: value and self._contains_numeric(value),
            transform=self._extract_numeric,
            validation=lambda value: self._is_numeric(value),
            examples=[
                {"before": "$1,234.56", "after": 1234.56},
                {"before": "약 100개", "after": 100}
            ]
        ))
    
    def add_rule(self, rule: CleaningRule):
        """정리 규칙 추가"""
        self.rules.append(rule)
        self.logger.debug(f"정리 규칙 추가: {rule.name}")
    
    def clean_record(self, record: Dict[str, Any], record_id: Optional[str] = None) -> AutoFixResult:
        """단일 레코드 정리"""
        
        cleaned_record = record.copy()
        cleaning_results = []
        success_count = 0
        error_count = 0
        
        for field_name, value in record.items():
            field_results = self.clean_field(field_name, value)
            
            for result in field_results:
                cleaning_results.append(result)
                
                if result.success:
                    cleaned_record[field_name] = result.cleaned_value
                    success_count += 1
                else:
                    error_count += 1
        
        return AutoFixResult(
            original_record=record,
            cleaned_record=cleaned_record,
            cleaning_results=cleaning_results,
            success_count=success_count,
            error_count=error_count,
            record_id=record_id
        )
    
    def clean_field(self, field_name: str, value: Any) -> List[CleaningResult]:
        """단일 필드 정리"""
        
        results = []
        current_value = value
        
        # 해당 필드에 적용할 규칙들 찾기
        applicable_rules = self._find_applicable_rules(field_name)
        
        for rule in applicable_rules:
            try:
                if rule.condition(current_value):
                    original_value = current_value
                    transformed_value = rule.transform(current_value)
                    
                    # 변환 후 검증
                    success = True
                    error_message = None
                    
                    if rule.validation:
                        if not rule.validation(transformed_value):
                            success = False
                            error_message = f"변환 후 검증 실패: {rule.name}"
                    
                    result = CleaningResult(
                        field_name=field_name,
                        rule_name=rule.name,
                        action=rule.action,
                        severity=rule.severity,
                        original_value=original_value,
                        cleaned_value=transformed_value,
                        success=success,
                        error_message=error_message
                    )
                    
                    results.append(result)
                    
                    # 성공한 경우 현재 값 업데이트
                    if success:
                        current_value = transformed_value
                
            except Exception as e:
                self.logger.error(f"정리 규칙 실행 오류 ({field_name}, {rule.name}): {e}")
                results.append(CleaningResult(
                    field_name=field_name,
                    rule_name=rule.name,
                    action=rule.action,
                    severity=rule.severity,
                    original_value=value,
                    cleaned_value=value,
                    success=False,
                    error_message=f"규칙 실행 오류: {e}"
                ))
        
        return results
    
    def clean_dataset(self, dataset: List[Dict[str, Any]], id_field: str = "id") -> List[AutoFixResult]:
        """데이터셋 정리"""
        
        self.logger.info(f"데이터셋 정리 시작: {len(dataset)}개 레코드")
        
        results = []
        for i, record in enumerate(dataset):
            record_id = record.get(id_field, f"record_{i}")
            result = self.clean_record(record, str(record_id))
            results.append(result)
        
        total_success = sum(r.success_count for r in results)
        total_errors = sum(r.error_count for r in results)
        
        self.logger.info(
            f"데이터셋 정리 완료: {total_success}개 성공, {total_errors}개 오류"
        )
        
        return results
    
    def _find_applicable_rules(self, field_name: str) -> List[CleaningRule]:
        """필드에 적용할 수 있는 규칙들 찾기"""
        
        applicable_rules = []
        
        for rule in self.rules:
            if self._field_matches_pattern(field_name, rule.field_pattern):
                applicable_rules.append(rule)
        
        # 심각도 순으로 정렬 (낮은 것부터)
        applicable_rules.sort(key=lambda r: list(CleaningSeverity).index(r.severity))
        
        return applicable_rules
    
    def _field_matches_pattern(self, field_name: str, pattern: str) -> bool:
        """필드명이 패턴과 일치하는지 확인"""
        
        if pattern == "*":
            return True
        
        if "*" in pattern:
            # 와일드카드 패턴 변환
            regex_pattern = pattern.replace("*", ".*")
            return bool(re.match(regex_pattern, field_name, re.IGNORECASE))
        else:
            return field_name.lower() == pattern.lower()
    
    # ========== 변환 함수들 ==========
    
    def _normalize_phone_number(self, phone: str) -> str:
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
            if digits.startswith('02'):
                return f"{digits[:2]}-{digits[2:6]}-{digits[6:]}"
            else:
                return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
        elif len(digits) == 9:
            return f"{digits[:2]}-{digits[2:5]}-{digits[5:]}"
        
        return phone  # 변환할 수 없으면 원본 반환
    
    def _normalize_url(self, url: str) -> str:
        """URL 정규화"""
        if not url:
            return url
        
        url = str(url).strip().lower()
        
        # 프로토콜 추가
        if not url.startswith(('http://', 'https://')):
            if url.startswith('www.'):
                url = 'http://' + url
            elif '.' in url:
                url = 'http://' + url
        
        # 마지막 슬래시 제거
        if url.endswith('/') and url.count('/') > 2:
            url = url.rstrip('/')
        
        return url
    
    def _normalize_date(self, date_str: str) -> str:
        """날짜 정규화"""
        if not date_str:
            return date_str
        
        date_str = str(date_str).strip()
        
        # 다양한 날짜 형식 시도
        patterns = [
            (r'(\d{4})[/.-](\d{1,2})[/.-](\d{1,2})', r'\1-\2-\3'),
            (r'(\d{1,2})[/.-](\d{1,2})[/.-](\d{4})', r'\3-\1-\2'),
            (r'(\d{4})(\d{2})(\d{2})', r'\1-\2-\3'),
        ]
        
        for pattern, replacement in patterns:
            match = re.match(pattern, date_str)
            if match:
                normalized = re.sub(pattern, replacement, date_str)
                # 월/일이 한 자리인 경우 0 패딩
                parts = normalized.split('-')
                if len(parts) == 3:
                    year, month, day = parts
                    return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
        
        return date_str
    
    def _remove_html_tags(self, text: str) -> str:
        """HTML 태그 제거"""
        if not text:
            return text
        
        # HTML 태그 제거
        clean_text = re.sub(r'<[^>]+>', '', str(text))
        
        # HTML 엔티티 변환
        html_entities = {
            '&amp;': '&',
            '&lt;': '<',
            '&gt;': '>',
            '&quot;': '"',
            '&#39;': "'",
            '&nbsp;': ' '
        }
        
        for entity, char in html_entities.items():
            clean_text = clean_text.replace(entity, char)
        
        return clean_text.strip()
    
    def _extract_numeric(self, value: str) -> Union[int, float, str]:
        """숫자 추출"""
        if not value:
            return value
        
        text = str(value)
        
        # 숫자와 소수점, 쉼표만 추출
        numeric_text = re.sub(r'[^\d.,]', '', text)
        
        if not numeric_text:
            return value
        
        # 쉼표 제거
        numeric_text = numeric_text.replace(',', '')
        
        try:
            if '.' in numeric_text:
                return float(numeric_text)
            else:
                return int(numeric_text)
        except ValueError:
            return value
    
    # ========== 검증 함수들 ==========
    
    def _is_numeric(self, value: Any) -> bool:
        """숫자인지 확인"""
        try:
            float(value)
            return True
        except (ValueError, TypeError):
            return False
    
    def _contains_numeric(self, value: Any) -> bool:
        """숫자를 포함하는지 확인"""
        return bool(re.search(r'\d', str(value)))
    
    def _looks_like_date(self, value: Any) -> bool:
        """날짜처럼 보이는지 확인"""
        date_patterns = [
            r'\d{4}[/.-]\d{1,2}[/.-]\d{1,2}',
            r'\d{1,2}[/.-]\d{1,2}[/.-]\d{4}',
            r'\d{8}',
        ]
        
        text = str(value)
        return any(re.search(pattern, text) for pattern in date_patterns)
    
    def _is_valid_date_format(self, value: str) -> bool:
        """유효한 날짜 형식인지 확인"""
        try:
            if re.match(r'\d{4}-\d{2}-\d{2}', str(value)):
                year, month, day = map(int, str(value).split('-'))
                datetime(year, month, day)
                return True
        except (ValueError, TypeError):
            pass
        return False
    
    # ========== 유틸리티 메소드들 ==========
    
    def get_cleaning_summary(self, results: List[AutoFixResult]) -> Dict[str, Any]:
        """정리 요약 정보 생성"""
        
        total_records = len(results)
        total_cleanings = sum(r.success_count + r.error_count for r in results)
        total_success = sum(r.success_count for r in results)
        total_errors = sum(r.error_count for r in results)
        
        # 규칙별 통계
        rule_stats = {}
        action_stats = {}
        severity_stats = {}
        
        for result in results:
            for cleaning in result.cleaning_results:
                # 규칙별
                if cleaning.rule_name not in rule_stats:
                    rule_stats[cleaning.rule_name] = {'success': 0, 'error': 0}
                
                if cleaning.success:
                    rule_stats[cleaning.rule_name]['success'] += 1
                else:
                    rule_stats[cleaning.rule_name]['error'] += 1
                
                # 액션별
                action_key = cleaning.action.value
                if action_key not in action_stats:
                    action_stats[action_key] = {'success': 0, 'error': 0}
                
                if cleaning.success:
                    action_stats[action_key]['success'] += 1
                else:
                    action_stats[action_key]['error'] += 1
                
                # 심각도별
                severity_key = cleaning.severity.value
                if severity_key not in severity_stats:
                    severity_stats[severity_key] = {'success': 0, 'error': 0}
                
                if cleaning.success:
                    severity_stats[severity_key]['success'] += 1
                else:
                    severity_stats[severity_key]['error'] += 1
        
        return {
            'total_records': total_records,
            'total_cleanings': total_cleanings,
            'success_rate': total_success / total_cleanings if total_cleanings > 0 else 0,
            'total_success': total_success,
            'total_errors': total_errors,
            'rule_statistics': rule_stats,
            'action_statistics': action_stats,
            'severity_statistics': severity_stats
        }
    
    def get_cleaned_dataset(self, results: List[AutoFixResult]) -> List[Dict[str, Any]]:
        """정리된 데이터셋 반환"""
        return [result.cleaned_record for result in results]
    
    def get_failed_cleanings(self, results: List[AutoFixResult]) -> List[CleaningResult]:
        """실패한 정리 작업들 반환"""
        failed_cleanings = []
        for result in results:
            for cleaning in result.cleaning_results:
                if not cleaning.success:
                    failed_cleanings.append(cleaning)
        return failed_cleanings