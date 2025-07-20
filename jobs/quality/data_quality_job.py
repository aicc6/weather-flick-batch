"""
데이터 품질 검사 배치 작업

시스템 내 모든 테이블의 데이터 품질을 검사하고 이상 상황을 감지하는 배치 작업입니다.
"""

import os
import json
from datetime import datetime
from typing import Dict, List, Optional
from dataclasses import dataclass
from enum import Enum

from app.core.base_job import BaseJob, JobResult, JobConfig
from app.core.database_manager import DatabaseManager


class QualityIssueType(Enum):
    """데이터 품질 문제 유형"""

    MISSING_DATA = "missing_data"
    DUPLICATE_DATA = "duplicate_data"
    INVALID_FORMAT = "invalid_format"
    OUTDATED_DATA = "outdated_data"
    INCONSISTENT_DATA = "inconsistent_data"
    THRESHOLD_VIOLATION = "threshold_violation"


@dataclass
class QualityIssue:
    """데이터 품질 문제"""

    issue_type: QualityIssueType
    table_name: str
    column_name: Optional[str]
    severity: str  # critical, high, medium, low
    description: str
    count: int
    recommendation: str


@dataclass
class TableQualityResult:
    """테이블별 품질 검사 결과"""

    table_name: str
    total_records: int
    missing_data_count: int
    duplicate_count: int
    quality_score: float
    issues: List[QualityIssue]
    check_timestamp: datetime


class DataQualityJob(BaseJob):
    """데이터 품질 검사 배치 작업"""

    def __init__(self, config: JobConfig):
        super().__init__(config)
        self.db_manager = DatabaseManager()
        self.processed_tables = 0

        # 품질 검사 대상 테이블 정의 (외부 JSON 파일에서 로드)
        self.quality_checks = self._load_quality_checks_config()

    def _load_quality_checks_config(self) -> Dict:
        """외부 JSON 파일에서 품질 검사 설정을 로드합니다."""
        config_path = os.path.join(
            os.path.dirname(__file__), "../../config", "quality_checks.json"
        )
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except FileNotFoundError:
            self.logger.error(f"품질 검사 설정 파일이 없습니다: {config_path}")
            return {}
        except json.JSONDecodeError as e:
            self.logger.error(f"품질 검사 설정 파일 파싱 오류: {e}")
            return {}

    def execute(self) -> JobResult:
        """데이터 품질 검사 실행"""
        from app.core.base_job import JobStatus

        result = JobResult(
            job_name=self.config.job_name,
            job_type=self.config.job_type,
            status=JobStatus.RUNNING,
            start_time=datetime.now(),
        )

        try:
            quality_results = []
            total_issues = 0

            # 각 테이블별 품질 검사 수행
            for table_name, check_config in self.quality_checks.items():
                try:
                    table_result = self._check_table_quality(table_name, check_config)
                    quality_results.append(table_result)
                    total_issues += len(table_result.issues)
                    self.processed_tables += 1

                    self.logger.info(
                        f"테이블 {table_name} 품질 검사 완료: "
                        f"점수 {table_result.quality_score:.1f}/100, "
                        f"문제 {len(table_result.issues)}건"
                    )
                except Exception as e:
                    self.logger.warning(f"테이블 {table_name} 품질 검사 실패: {str(e)}")

            # 품질 검사 결과 저장
            saved_count = self._save_quality_results(quality_results)

            # 심각한 문제 알림
            critical_issues = self._get_critical_issues(quality_results)
            if critical_issues:
                self._notify_critical_issues(critical_issues)

            # 결과 업데이트 (상태는 BaseJob.run에서 설정)
            result.processed_records = saved_count
            result.metadata = {
                "tables_checked": self.processed_tables,
                "total_issues": total_issues,
                "critical_issues": len(critical_issues),
                "avg_quality_score": (
                    sum(r.quality_score for r in quality_results) / len(quality_results)
                    if quality_results
                    else 0
                ),
            }

            self.logger.info(
                f"데이터 품질 검사 완료: {self.processed_tables}개 테이블, {total_issues}개 문제 발견"
            )

        except Exception as e:
            self.logger.error(f"데이터 품질 검사 실패: {str(e)}")
            result.error_message = str(e)
            raise

        return result

    def _check_table_quality(
        self, table_name: str, check_config: Dict
    ) -> TableQualityResult:
        """특정 테이블의 품질 검사"""
        issues = []

        # 테이블 존재 여부 확인
        if not self._table_exists(table_name):
            issue = QualityIssue(
                issue_type=QualityIssueType.MISSING_DATA,
                table_name=table_name,
                column_name=None,
                severity="critical",
                description=f"테이블 {table_name}이 존재하지 않음",
                count=1,
                recommendation="테이블 생성 또는 스키마 확인 필요",
            )
            issues.append(issue)
            return TableQualityResult(
                table_name=table_name,
                total_records=0,
                missing_data_count=0,
                duplicate_count=0,
                quality_score=0.0,
                issues=issues,
                check_timestamp=datetime.now(),
            )

        # 기본 통계 수집
        total_records = self._get_record_count(table_name)
        missing_data_count = self._check_missing_data(
            table_name, check_config["required_columns"]
        )
        duplicate_count = self._check_duplicates(
            table_name, check_config["duplicate_key_columns"]
        )

        # 데이터 신선도 검사
        freshness_issues = self._check_data_freshness(
            table_name,
            check_config["date_column"],
            check_config["freshness_threshold_days"],
        )
        issues.extend(freshness_issues)

        # 값 범위 검사
        range_issues = self._check_value_ranges(
            table_name, check_config["value_ranges"]
        )
        issues.extend(range_issues)

        # 일관성 검사
        consistency_issues = self._check_data_consistency(table_name)
        issues.extend(consistency_issues)

        # 품질 점수 계산 (0-100)
        quality_score = self._calculate_quality_score(
            total_records, missing_data_count, duplicate_count, issues
        )

        return TableQualityResult(
            table_name=table_name,
            total_records=total_records,
            missing_data_count=missing_data_count,
            duplicate_count=duplicate_count,
            quality_score=quality_score,
            issues=issues,
            check_timestamp=datetime.now(),
        )

    def _table_exists(self, table_name: str) -> bool:
        """테이블 존재 여부 확인"""
        query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_name = %s AND table_schema = 'public'
        )
        """
        try:
            result = self.db_manager.execute_query(query, (table_name,))
            return result[0]["exists"] if result else False
        except:
            return False

    def _get_record_count(self, table_name: str) -> int:
        """테이블 레코드 수 조회"""
        try:
            query = f"SELECT COUNT(*) as count FROM {table_name}"
            result = self.db_manager.execute_query(query)
            return result[0]["count"] if result else 0
        except:
            return 0

    def _check_missing_data(self, table_name: str, required_columns: List[str]) -> int:
        """필수 컬럼의 NULL 데이터 검사"""
        total_missing = 0
        for column in required_columns:
            try:
                query = (
                    f"SELECT COUNT(*) as count FROM {table_name} WHERE {column} IS NULL"
                )
                result = self.db_manager.execute_query(query)
                total_missing += result[0]["count"] if result else 0
            except:
                continue
        return total_missing

    def _check_duplicates(self, table_name: str, key_columns: List[str]) -> int:
        """중복 데이터 검사"""
        if not key_columns:
            return 0

        try:
            columns_str = ", ".join(key_columns)
            query = f"""
            SELECT COUNT(*) as duplicate_count FROM (
                SELECT {columns_str}, COUNT(*) as cnt
                FROM {table_name}
                GROUP BY {columns_str}
                HAVING COUNT(*) > 1
            ) duplicates
            """
            result = self.db_manager.execute_query(query)
            return result[0]["duplicate_count"] if result else 0
        except:
            return 0

    def _check_data_freshness(
        self, table_name: str, date_column: str, threshold_days: int
    ) -> List[QualityIssue]:
        """데이터 신선도 검사"""
        issues = []
        try:
            query = f"""
            SELECT COUNT(*) as old_count
            FROM {table_name}
            WHERE {date_column} < CURRENT_DATE - INTERVAL '{threshold_days} days'
            """
            result = self.db_manager.execute_query(query)
            old_count = result[0]["old_count"] if result else 0

            if old_count > 0:
                issue = QualityIssue(
                    issue_type=QualityIssueType.OUTDATED_DATA,
                    table_name=table_name,
                    column_name=date_column,
                    severity="medium",
                    description=f"{threshold_days}일 이전의 오래된 데이터 발견",
                    count=old_count,
                    recommendation="오래된 데이터 정리 또는 업데이트 주기 검토",
                )
                issues.append(issue)
        except:
            pass
        return issues

    def _check_value_ranges(
        self, table_name: str, value_ranges: Dict[str, tuple]
    ) -> List[QualityIssue]:
        """값 범위 검사"""
        issues = []
        for column, (min_val, max_val) in value_ranges.items():
            try:
                query = f"""
                SELECT COUNT(*) as invalid_count
                FROM {table_name}
                WHERE {column} IS NOT NULL
                AND ({column} < {min_val} OR {column} > {max_val})
                """
                result = self.db_manager.execute_query(query)
                invalid_count = result[0]["invalid_count"] if result else 0

                if invalid_count > 0:
                    issue = QualityIssue(
                        issue_type=QualityIssueType.INVALID_FORMAT,
                        table_name=table_name,
                        column_name=column,
                        severity="high",
                        description=f"유효 범위({min_val}-{max_val}) 벗어난 값 발견",
                        count=invalid_count,
                        recommendation="데이터 수집 로직 검토 및 유효성 검증 강화",
                    )
                    issues.append(issue)
            except:
                continue
        return issues

    def _check_data_consistency(self, table_name: str) -> List[QualityIssue]:
        """데이터 일관성 검사"""
        issues = []

        # 날씨 데이터 특화 일관성 검사
        if table_name == "historical_weather_daily":
            try:
                # 최고기온 < 최저기온 검사
                query = """
                SELECT COUNT(*) as inconsistent_count
                FROM historical_weather_daily
                WHERE max_temp IS NOT NULL AND min_temp IS NOT NULL
                AND max_temp < min_temp
                """
                result = self.db_manager.execute_query(query)
                inconsistent_count = result[0]["inconsistent_count"] if result else 0

                if inconsistent_count > 0:
                    issue = QualityIssue(
                        issue_type=QualityIssueType.INCONSISTENT_DATA,
                        table_name=table_name,
                        column_name="max_temp,min_temp",
                        severity="high",
                        description="최고기온이 최저기온보다 낮은 데이터 발견",
                        count=inconsistent_count,
                        recommendation="온도 데이터 수집 로직 검토",
                    )
                    issues.append(issue)
            except:
                pass

        return issues

    def _calculate_quality_score(
        self,
        total_records: int,
        missing_count: int,
        duplicate_count: int,
        issues: List[QualityIssue],
    ) -> float:
        """품질 점수 계산 (0-100)"""
        if total_records == 0:
            return 0.0

        # 기본 점수에서 문제별로 감점
        score = 100.0

        # 누락 데이터 감점 (최대 -30점)
        if missing_count > 0:
            missing_penalty = min(30, (missing_count / total_records) * 100)
            score -= missing_penalty

        # 중복 데이터 감점 (최대 -20점)
        if duplicate_count > 0:
            duplicate_penalty = min(20, (duplicate_count / total_records) * 100)
            score -= duplicate_penalty

        # 품질 이슈별 감점
        for issue in issues:
            if issue.severity == "critical":
                score -= 15
            elif issue.severity == "high":
                score -= 10
            elif issue.severity == "medium":
                score -= 5
            elif issue.severity == "low":
                score -= 2

        return max(0.0, score)

    def _save_quality_results(self, quality_results: List[TableQualityResult]) -> int:
        """품질 검사 결과 저장"""
        saved_count = 0

        for result in quality_results:
            try:
                # issues를 JSON으로 직렬화
                issues_json = json.dumps(
                    [
                        {
                            "issue_type": issue.issue_type.value,
                            "column_name": issue.column_name,
                            "severity": issue.severity,
                            "description": issue.description,
                            "count": issue.count,
                            "recommendation": issue.recommendation,
                        }
                        for issue in result.issues
                    ],
                    ensure_ascii=False,
                )

                query = """
                INSERT INTO data_quality_checks
                (table_name, check_date, total_records, missing_data_count,
                 duplicate_count, quality_score, issues)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """

                self.db_manager.execute_update(
                    query,
                    (
                        result.table_name,
                        result.check_timestamp.date(),
                        result.total_records,
                        result.missing_data_count,
                        result.duplicate_count,
                        result.quality_score,
                        issues_json,
                    ),
                )
                saved_count += 1
            except Exception as e:
                self.logger.warning(
                    f"품질 결과 저장 실패 [{result.table_name}]: {str(e)}"
                )

        return saved_count

    def _get_critical_issues(
        self, quality_results: List[TableQualityResult]
    ) -> List[QualityIssue]:
        """심각한 품질 문제 필터링"""
        critical_issues = []
        for result in quality_results:
            for issue in result.issues:
                if issue.severity in ["critical", "high"]:
                    critical_issues.append(issue)
        return critical_issues

    def _notify_critical_issues(self, critical_issues: List[QualityIssue]) -> None:
        """심각한 품질 문제 알림"""
        # 로그에 심각한 문제 기록
        self.logger.warning(f"심각한 데이터 품질 문제 {len(critical_issues)}건 발견:")
        for issue in critical_issues:
            self.logger.warning(
                f"- {issue.table_name}.{issue.column_name}: {issue.description} "
                f"({issue.count}건, {issue.severity})"
            )

    def pre_execute(self) -> bool:
        """실행 전 검증"""
        try:
            # 데이터베이스 연결 확인
            self.db_manager.execute_query("SELECT 1")
            return True
        except Exception as e:
            self.logger.error(f"데이터베이스 연결 실패: {e}")
            return False

    def post_execute(self, result: JobResult) -> None:
        """실행 후 처리"""
        super().post_execute(result)

        # 품질 검사 요약 리포트
        if result.metadata:
            avg_score = result.metadata.get("avg_quality_score", 0)
            critical_issues = result.metadata.get("critical_issues", 0)

            self.logger.info(
                f"데이터 품질 요약: 평균 점수 {avg_score:.1f}/100, "
                f"심각한 문제 {critical_issues}건"
            )

            # 품질 점수가 낮으면 경고
            if avg_score < 70:
                self.logger.warning(
                    "전체 데이터 품질 점수가 낮습니다. 데이터 정리가 필요합니다."
                )
