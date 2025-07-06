"""
중복 데이터 감지 및 제거 시스템

데이터셋에서 중복된 레코드를 감지하고 제거하는 기능을 제공합니다.
"""

import hashlib
import logging
from typing import Dict, List, Any, Optional, Set, Tuple, Callable
from dataclasses import dataclass, field
from enum import Enum
import json
from collections import defaultdict


class DuplicateStrategy(Enum):
    """중복 처리 전략"""
    KEEP_FIRST = "keep_first"        # 첫 번째 유지
    KEEP_LAST = "keep_last"          # 마지막 유지
    KEEP_BEST = "keep_best"          # 품질이 좋은 것 유지
    MERGE = "merge"                  # 병합
    MARK_ONLY = "mark_only"          # 표시만 (제거하지 않음)


class DuplicateType(Enum):
    """중복 유형"""
    EXACT = "exact"                  # 완전 일치
    FUZZY = "fuzzy"                  # 유사 일치
    FIELD_BASED = "field_based"      # 특정 필드 기준
    SEMANTIC = "semantic"            # 의미적 유사성


@dataclass
class DuplicateConfig:
    """중복 감지 설정"""
    
    # 기본 설정
    strategy: DuplicateStrategy = DuplicateStrategy.KEEP_FIRST
    duplicate_type: DuplicateType = DuplicateType.EXACT
    
    # 필드 기준 중복 감지
    key_fields: List[str] = field(default_factory=list)  # 중복 판단 기준 필드
    ignore_fields: List[str] = field(default_factory=list)  # 무시할 필드
    
    # 유사성 임계값
    similarity_threshold: float = 0.9
    
    # 품질 평가 기준 (KEEP_BEST 전략용)
    quality_fields: List[str] = field(default_factory=list)
    quality_weights: Dict[str, float] = field(default_factory=dict)
    
    # 병합 설정 (MERGE 전략용)
    merge_rules: Dict[str, str] = field(default_factory=dict)  # field: strategy


@dataclass
class DuplicateGroup:
    """중복 그룹"""
    group_id: str
    records: List[Dict[str, Any]]
    record_indices: List[int]
    duplicate_type: DuplicateType
    similarity_score: float
    key_values: Dict[str, Any]


@dataclass
class DuplicateResult:
    """중복 감지 결과"""
    original_count: int
    duplicate_groups: List[DuplicateGroup]
    total_duplicates: int
    unique_records: List[Dict[str, Any]]
    removed_records: List[Dict[str, Any]]
    merged_records: List[Dict[str, Any]]
    duplicate_summary: Dict[str, Any]


class DuplicateDetector:
    """중복 데이터 감지기"""
    
    def __init__(self, config: DuplicateConfig = None):
        self.config = config or DuplicateConfig()
        self.logger = logging.getLogger(__name__)
    
    def detect_duplicates(self, dataset: List[Dict[str, Any]]) -> DuplicateResult:
        """중복 데이터 감지"""
        
        self.logger.info(f"중복 감지 시작: {len(dataset)}개 레코드")
        
        if self.config.duplicate_type == DuplicateType.EXACT:
            duplicate_groups = self._detect_exact_duplicates(dataset)
        elif self.config.duplicate_type == DuplicateType.FIELD_BASED:
            duplicate_groups = self._detect_field_based_duplicates(dataset)
        elif self.config.duplicate_type == DuplicateType.FUZZY:
            duplicate_groups = self._detect_fuzzy_duplicates(dataset)
        else:
            duplicate_groups = self._detect_exact_duplicates(dataset)  # 기본값
        
        # 중복 처리 전략 적용
        unique_records, removed_records, merged_records = self._apply_strategy(dataset, duplicate_groups)
        
        # 요약 정보 생성
        summary = self._create_summary(dataset, duplicate_groups)
        
        result = DuplicateResult(
            original_count=len(dataset),
            duplicate_groups=duplicate_groups,
            total_duplicates=sum(len(group.records) - 1 for group in duplicate_groups),
            unique_records=unique_records,
            removed_records=removed_records,
            merged_records=merged_records,
            duplicate_summary=summary
        )
        
        self.logger.info(
            f"중복 감지 완료: {len(duplicate_groups)}개 그룹, "
            f"{result.total_duplicates}개 중복 발견"
        )
        
        return result
    
    def _detect_exact_duplicates(self, dataset: List[Dict[str, Any]]) -> List[DuplicateGroup]:
        """완전 일치 중복 감지"""
        
        hash_groups = defaultdict(list)
        
        for i, record in enumerate(dataset):
            # 무시할 필드 제외하고 해시 생성
            filtered_record = self._filter_record(record)
            record_hash = self._calculate_hash(filtered_record)
            hash_groups[record_hash].append((i, record))
        
        # 중복 그룹 생성 (2개 이상인 경우만)
        duplicate_groups = []
        group_id = 0
        
        for record_hash, records in hash_groups.items():
            if len(records) > 1:
                group = DuplicateGroup(
                    group_id=f"exact_{group_id}",
                    records=[record for _, record in records],
                    record_indices=[i for i, _ in records],
                    duplicate_type=DuplicateType.EXACT,
                    similarity_score=1.0,
                    key_values={"hash": record_hash}
                )
                duplicate_groups.append(group)
                group_id += 1
        
        return duplicate_groups
    
    def _detect_field_based_duplicates(self, dataset: List[Dict[str, Any]]) -> List[DuplicateGroup]:
        """필드 기반 중복 감지"""
        
        if not self.config.key_fields:
            self.logger.warning("key_fields가 설정되지 않아 전체 필드로 중복 감지합니다.")
            return self._detect_exact_duplicates(dataset)
        
        key_groups = defaultdict(list)
        
        for i, record in enumerate(dataset):
            # 키 필드 값들 추출
            key_values = {}
            for field in self.config.key_fields:
                key_values[field] = record.get(field)
            
            # 키 값들로 해시 생성
            key_hash = self._calculate_hash(key_values)
            key_groups[key_hash].append((i, record, key_values))
        
        # 중복 그룹 생성
        duplicate_groups = []
        group_id = 0
        
        for key_hash, records in key_groups.items():
            if len(records) > 1:
                group = DuplicateGroup(
                    group_id=f"field_{group_id}",
                    records=[record for _, record, _ in records],
                    record_indices=[i for i, _, _ in records],
                    duplicate_type=DuplicateType.FIELD_BASED,
                    similarity_score=1.0,
                    key_values=records[0][2]  # 첫 번째 레코드의 키 값들
                )
                duplicate_groups.append(group)
                group_id += 1
        
        return duplicate_groups
    
    def _detect_fuzzy_duplicates(self, dataset: List[Dict[str, Any]]) -> List[DuplicateGroup]:
        """유사 일치 중복 감지"""
        
        duplicate_groups = []
        processed_indices = set()
        group_id = 0
        
        for i, record1 in enumerate(dataset):
            if i in processed_indices:
                continue
            
            similar_records = [(i, record1)]
            current_group_indices = {i}
            
            for j, record2 in enumerate(dataset[i+1:], i+1):
                if j in processed_indices:
                    continue
                
                similarity = self._calculate_similarity(record1, record2)
                if similarity >= self.config.similarity_threshold:
                    similar_records.append((j, record2))
                    current_group_indices.add(j)
            
            # 유사한 레코드가 2개 이상인 경우만 중복 그룹 생성
            if len(similar_records) > 1:
                group = DuplicateGroup(
                    group_id=f"fuzzy_{group_id}",
                    records=[record for _, record in similar_records],
                    record_indices=[idx for idx, _ in similar_records],
                    duplicate_type=DuplicateType.FUZZY,
                    similarity_score=self.config.similarity_threshold,
                    key_values=self._extract_representative_values(similar_records)
                )
                duplicate_groups.append(group)
                processed_indices.update(current_group_indices)
                group_id += 1
        
        return duplicate_groups
    
    def _apply_strategy(self, dataset: List[Dict[str, Any]], duplicate_groups: List[DuplicateGroup]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
        """중복 처리 전략 적용"""
        
        if self.config.strategy == DuplicateStrategy.MARK_ONLY:
            return dataset.copy(), [], []
        
        # 제거할 인덱스 수집
        indices_to_remove = set()
        merged_records = []
        
        for group in duplicate_groups:
            if self.config.strategy == DuplicateStrategy.KEEP_FIRST:
                # 첫 번째 제외하고 모두 제거
                indices_to_remove.update(group.record_indices[1:])
                
            elif self.config.strategy == DuplicateStrategy.KEEP_LAST:
                # 마지막 제외하고 모두 제거
                indices_to_remove.update(group.record_indices[:-1])
                
            elif self.config.strategy == DuplicateStrategy.KEEP_BEST:
                # 품질이 가장 좋은 것 제외하고 모두 제거
                best_index = self._find_best_record_index(group)
                indices_to_remove.update(idx for idx in group.record_indices if idx != best_index)
                
            elif self.config.strategy == DuplicateStrategy.MERGE:
                # 병합된 레코드 생성
                merged_record = self._merge_records(group.records)
                merged_records.append(merged_record)
                # 원본 레코드들은 모두 제거
                indices_to_remove.update(group.record_indices)
        
        # 제거되지 않을 레코드들 수집
        unique_records = []
        removed_records = []
        
        for i, record in enumerate(dataset):
            if i in indices_to_remove:
                removed_records.append(record)
            else:
                unique_records.append(record)
        
        # 병합된 레코드들 추가
        unique_records.extend(merged_records)
        
        return unique_records, removed_records, merged_records
    
    def _filter_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """무시할 필드 제외한 레코드 반환"""
        if not self.config.ignore_fields:
            return record
        
        return {k: v for k, v in record.items() if k not in self.config.ignore_fields}
    
    def _calculate_hash(self, obj: Any) -> str:
        """객체의 해시값 계산"""
        try:
            # 딕셔너리를 정렬된 JSON 문자열로 변환 후 해시
            json_str = json.dumps(obj, sort_keys=True, ensure_ascii=False, default=str)
            return hashlib.md5(json_str.encode('utf-8')).hexdigest()
        except Exception as e:
            self.logger.warning(f"해시 계산 실패: {e}")
            return str(hash(str(obj)))
    
    def _calculate_similarity(self, record1: Dict[str, Any], record2: Dict[str, Any]) -> float:
        """두 레코드 간 유사도 계산"""
        
        # 간단한 자카드 유사도 계산
        record1_filtered = self._filter_record(record1)
        record2_filtered = self._filter_record(record2)
        
        # 공통 필드만 비교
        common_fields = set(record1_filtered.keys()) & set(record2_filtered.keys())
        if not common_fields:
            return 0.0
        
        matching_fields = 0
        for field in common_fields:
            value1 = record1_filtered.get(field)
            value2 = record2_filtered.get(field)
            
            if value1 == value2:
                matching_fields += 1
            elif isinstance(value1, str) and isinstance(value2, str):
                # 문자열 유사도 계산 (간단한 방법)
                string_similarity = self._calculate_string_similarity(value1, value2)
                if string_similarity > 0.8:
                    matching_fields += string_similarity
        
        return matching_fields / len(common_fields)
    
    def _calculate_string_similarity(self, str1: str, str2: str) -> float:
        """문자열 유사도 계산 (레벤시테인 거리 기반)"""
        if str1 == str2:
            return 1.0
        
        if not str1 or not str2:
            return 0.0
        
        # 간단한 레벤시테인 거리 구현
        len1, len2 = len(str1), len(str2)
        if len1 > len2:
            str1, str2 = str2, str1
            len1, len2 = len2, len1
        
        current_row = list(range(len1 + 1))
        for i in range(1, len2 + 1):
            previous_row, current_row = current_row, [i] + [0] * len1
            for j in range(1, len1 + 1):
                add = previous_row[j] + 1
                delete = current_row[j - 1] + 1
                change = previous_row[j - 1]
                if str1[j - 1] != str2[i - 1]:
                    change += 1
                current_row[j] = min(add, delete, change)
        
        distance = current_row[len1]
        similarity = 1 - (distance / max(len1, len2))
        return max(0, similarity)
    
    def _find_best_record_index(self, group: DuplicateGroup) -> int:
        """품질이 가장 좋은 레코드의 인덱스 찾기"""
        
        if not self.config.quality_fields:
            # 품질 기준이 없으면 첫 번째 반환
            return group.record_indices[0]
        
        best_score = -1
        best_index = group.record_indices[0]
        
        for i, record in enumerate(group.records):
            score = self._calculate_quality_score(record)
            if score > best_score:
                best_score = score
                best_index = group.record_indices[i]
        
        return best_index
    
    def _calculate_quality_score(self, record: Dict[str, Any]) -> float:
        """레코드의 품질 점수 계산"""
        
        total_score = 0.0
        total_weight = 0.0
        
        for field in self.config.quality_fields:
            if field in record:
                value = record[field]
                weight = self.config.quality_weights.get(field, 1.0)
                
                # 기본적인 품질 평가 기준
                field_score = 0.0
                
                if value is not None and str(value).strip():
                    field_score = 1.0  # 값이 있으면 1점
                    
                    # 추가 품질 기준
                    if isinstance(value, str):
                        # 긴 문자열이 더 품질이 좋다고 가정
                        field_score += min(len(value) / 100, 1.0)
                
                total_score += field_score * weight
                total_weight += weight
        
        return total_score / total_weight if total_weight > 0 else 0.0
    
    def _merge_records(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """레코드들을 병합"""
        
        if not records:
            return {}
        
        merged = records[0].copy()
        
        for record in records[1:]:
            for field, value in record.items():
                if field not in merged or not merged[field]:
                    # 비어있는 필드는 값으로 채움
                    merged[field] = value
                elif field in self.config.merge_rules:
                    # 특별한 병합 규칙이 있는 경우
                    rule = self.config.merge_rules[field]
                    merged[field] = self._apply_merge_rule(merged[field], value, rule)
                else:
                    # 기본적으로는 첫 번째 값 유지
                    pass
        
        return merged
    
    def _apply_merge_rule(self, value1: Any, value2: Any, rule: str) -> Any:
        """병합 규칙 적용"""
        
        if rule == "concat":
            return str(value1) + " " + str(value2)
        elif rule == "max":
            try:
                return max(value1, value2)
            except (TypeError, ValueError):
                return value1
        elif rule == "min":
            try:
                return min(value1, value2)
            except (TypeError, ValueError):
                return value1
        elif rule == "latest":
            return value2  # 나중 값 사용
        elif rule == "longest":
            return value1 if len(str(value1)) > len(str(value2)) else value2
        else:
            return value1  # 기본값
    
    def _extract_representative_values(self, similar_records: List[Tuple[int, Dict[str, Any]]]) -> Dict[str, Any]:
        """유사한 레코드들의 대표값 추출"""
        
        if not similar_records:
            return {}
        
        # 첫 번째 레코드의 키 값들을 대표값으로 사용
        return {field: similar_records[0][1].get(field) for field in self.config.key_fields}
    
    def _create_summary(self, dataset: List[Dict[str, Any]], duplicate_groups: List[DuplicateGroup]) -> Dict[str, Any]:
        """중복 감지 요약 생성"""
        
        total_duplicates = sum(len(group.records) - 1 for group in duplicate_groups)
        
        # 중복 유형별 통계
        type_stats = defaultdict(int)
        for group in duplicate_groups:
            type_stats[group.duplicate_type.value] += len(group.records) - 1
        
        # 필드별 중복 통계 (필드 기반 중복인 경우)
        field_stats = defaultdict(int)
        if self.config.duplicate_type == DuplicateType.FIELD_BASED:
            for group in duplicate_groups:
                for field in self.config.key_fields:
                    field_stats[field] += len(group.records) - 1
        
        return {
            'original_count': len(dataset),
            'duplicate_groups': len(duplicate_groups),
            'total_duplicates': total_duplicates,
            'unique_count': len(dataset) - total_duplicates,
            'duplicate_rate': total_duplicates / len(dataset) if dataset else 0,
            'strategy_used': self.config.strategy.value,
            'type_used': self.config.duplicate_type.value,
            'type_statistics': dict(type_stats),
            'field_statistics': dict(field_stats) if field_stats else None
        }
    
    def detect_and_remove(self, dataset: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """중복 감지 및 제거 (간편 메소드)"""
        result = self.detect_duplicates(dataset)
        return result.unique_records
    
    def get_duplicate_report(self, dataset: List[Dict[str, Any]]) -> Dict[str, Any]:
        """중복 감지 보고서 생성"""
        result = self.detect_duplicates(dataset)
        
        return {
            'summary': result.duplicate_summary,
            'duplicate_groups': [
                {
                    'group_id': group.group_id,
                    'type': group.duplicate_type.value,
                    'count': len(group.records),
                    'similarity': group.similarity_score,
                    'key_values': group.key_values,
                    'record_previews': [
                        {k: str(v)[:50] + '...' if len(str(v)) > 50 else v 
                         for k, v in record.items() if k in (self.config.key_fields or list(record.keys())[:3])}
                        for record in group.records[:3]  # 최대 3개만 미리보기
                    ]
                }
                for group in result.duplicate_groups[:10]  # 최대 10개 그룹만
            ]
        }