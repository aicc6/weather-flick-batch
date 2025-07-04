"""
로그 정리 및 아카이빙 배치 작업

시스템 로그 파일 정리 및 장기 보관을 위한 아카이빙 작업
실행 주기: 매일 새벽 1시
"""

import gzip
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any
import boto3
from botocore.exceptions import ClientError

from app.core.logger import get_logger
from config.batch_settings import get_log_settings, get_aws_settings
from utils.file_manager import FileManager


class LogCleanupJob:
    """로그 정리 및 아카이빙 작업"""

    def __init__(self):
        self.logger = get_logger(__name__)
        self.log_settings = get_log_settings()
        self.aws_settings = get_aws_settings()
        self.file_manager = FileManager()
        self.s3_client = None
        self.processed_files = 0
        self.compressed_files = 0
        self.archived_files = 0
        self.deleted_files = 0
        self.total_size_saved = 0

    def _init_s3_client(self):
        """S3 클라이언트 초기화"""
        if not self.s3_client:
            try:
                # AWS 인증 정보 확인
                if (
                    not self.aws_settings.access_key_id
                    or not self.aws_settings.secret_access_key
                ):
                    self.logger.warning(
                        "AWS 인증 정보가 설정되지 않았습니다. S3 기능을 건너뜁니다."
                    )
                    return False

                self.s3_client = boto3.client(
                    "s3",
                    aws_access_key_id=self.aws_settings.access_key_id,
                    aws_secret_access_key=self.aws_settings.secret_access_key,
                    region_name=self.aws_settings.region,
                )
                return True
            except Exception as e:
                self.logger.error(f"S3 클라이언트 초기화 실패: {e}")
                return False

    async def execute(self) -> Dict[str, Any]:
        """로그 정리 및 아카이빙 실행"""
        self.logger.info("로그 정리 및 아카이빙 작업 시작")

        try:
            # S3 클라이언트 초기화
            s3_enabled = self._init_s3_client()

            # 1. 30일 이상 된 로그 파일 압축
            await self._compress_old_logs(days=30)

            # 2. 90일 이상 된 압축 파일을 S3로 이동 (S3 사용 가능할 때만)
            if s3_enabled:
                await self._archive_to_s3(days=90)
                # 3. 1년 이상 된 아카이브 파일 삭제 (S3 사용 가능할 때만)
                await self._delete_old_archives(days=365)
            else:
                self.logger.info(
                    "S3가 비활성화되어 아카이빙 및 삭제 작업을 건너뜁니다."
                )

            # 4. 로그 테이블 파티션 관리
            self._manage_log_partitions()

            # 5. 디스크 사용량 체크
            disk_usage = await self._check_disk_usage()

            # 6. 결과 반환
            result = {
                "processed_files": self.processed_files,
                "compressed_files": self.compressed_files,
                "archived_files": self.archived_files,
                "deleted_files": self.deleted_files,
                "total_size_saved_mb": round(self.total_size_saved / 1024 / 1024, 2),
                "disk_usage_percent": disk_usage,
                "status": "completed",
            }

            # 디스크 사용량 경고
            if disk_usage > 80:
                await self._send_disk_usage_alert(disk_usage)
                result["warnings"] = [f"높은 디스크 사용률: {disk_usage}%"]

            self.logger.info(
                f"로그 정리 완료: 압축 {self.compressed_files}개, "
                f"아카이브 {self.archived_files}개, 삭제 {self.deleted_files}개"
            )
            return result

        except Exception as e:
            self.logger.error(f"로그 정리 작업 실패: {e}")
            raise

    async def _compress_old_logs(self, days: int):
        """오래된 로그 파일 압축"""
        try:
            cutoff_date = datetime.now() - timedelta(days=days)
            log_dir = Path(self.log_settings.log_directory)

            self.logger.info(f"{days}일 이상 된 로그 파일 압축 시작")

            # 로그 파일 검색 패턴들
            patterns = ["*.log", "*.out", "*.err"]

            for pattern in patterns:
                log_files = list(log_dir.glob(pattern))

                for log_file in log_files:
                    try:
                        # 파일 수정 시간 확인
                        file_mtime = datetime.fromtimestamp(log_file.stat().st_mtime)

                        if file_mtime < cutoff_date:
                            await self._compress_file(log_file)
                            self.compressed_files += 1

                        self.processed_files += 1

                    except Exception as e:
                        self.logger.warning(f"파일 압축 실패 [{log_file}]: {e}")
                        continue

            self.logger.info(f"로그 파일 압축 완료: {self.compressed_files}개")

        except Exception as e:
            self.logger.error(f"로그 압축 실패: {e}")
            raise

    async def _compress_file(self, file_path: Path):
        """개별 파일 압축"""
        try:
            compressed_path = file_path.with_suffix(file_path.suffix + ".gz")

            # 이미 압축된 파일이 있으면 건너뛰기
            if compressed_path.exists():
                return

            original_size = file_path.stat().st_size

            # 파일 압축
            with open(file_path, "rb") as f_in:
                with gzip.open(compressed_path, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)

            # 압축률 확인
            compressed_size = compressed_path.stat().st_size
            compression_ratio = (1 - compressed_size / original_size) * 100

            # 원본 파일 삭제
            file_path.unlink()

            self.total_size_saved += original_size - compressed_size

            self.logger.debug(
                f"파일 압축 완료: {file_path.name} "
                f"({original_size} -> {compressed_size} bytes, "
                f"{compression_ratio:.1f}% 절약)"
            )

        except Exception as e:
            self.logger.error(f"파일 압축 실패 [{file_path}]: {e}")
            raise

    async def _archive_to_s3(self, days: int):
        """압축 파일을 S3로 아카이브"""
        try:
            cutoff_date = datetime.now() - timedelta(days=days)
            log_dir = Path(self.log_settings.log_directory)

            self.logger.info(f"{days}일 이상 된 압축 파일 S3 아카이브 시작")

            # 압축 파일 검색
            compressed_files = list(log_dir.glob("*.gz"))

            for compressed_file in compressed_files:
                try:
                    # 파일 수정 시간 확인
                    file_mtime = datetime.fromtimestamp(compressed_file.stat().st_mtime)

                    if file_mtime < cutoff_date:
                        await self._upload_to_s3(compressed_file)
                        compressed_file.unlink()  # 로컬에서 삭제
                        self.archived_files += 1

                except Exception as e:
                    self.logger.warning(f"S3 업로드 실패 [{compressed_file}]: {e}")
                    continue

            self.logger.info(f"S3 아카이브 완료: {self.archived_files}개")

        except Exception as e:
            self.logger.error(f"S3 아카이브 실패: {e}")
            raise

    async def _upload_to_s3(self, file_path: Path):
        """개별 파일을 S3에 업로드"""
        try:
            # S3 키 생성 (날짜별 폴더 구조)
            file_date = datetime.fromtimestamp(file_path.stat().st_mtime)
            s3_key = f"logs/{file_date.strftime('%Y/%m/%d')}/{file_path.name}"

            # 파일 업로드
            self.s3_client.upload_file(
                str(file_path), self.aws_settings.s3_log_bucket, s3_key
            )

            # 업로드 확인
            try:
                self.s3_client.head_object(
                    Bucket=self.aws_settings.s3_log_bucket, Key=s3_key
                )
            except ClientError:
                raise Exception("S3 업로드 검증 실패")

            self.logger.debug(f"S3 업로드 완료: {s3_key}")

        except Exception as e:
            self.logger.error(f"S3 업로드 실패 [{file_path}]: {e}")
            raise

    async def _delete_old_archives(self, days: int):
        """오래된 S3 아카이브 삭제"""
        try:
            cutoff_date = datetime.now() - timedelta(days=days)

            self.logger.info(f"{days}일 이상 된 S3 아카이브 삭제 시작")

            # S3 객체 목록 조회
            paginator = self.s3_client.get_paginator("list_objects_v2")
            page_iterator = paginator.paginate(
                Bucket=self.aws_settings.s3_log_bucket, Prefix="logs/"
            )

            objects_to_delete = []

            for page in page_iterator:
                if "Contents" not in page:
                    continue

                for obj in page["Contents"]:
                    if obj["LastModified"].replace(tzinfo=None) < cutoff_date:
                        objects_to_delete.append({"Key": obj["Key"]})

            # 배치 삭제 (최대 1000개씩)
            if objects_to_delete:
                for i in range(0, len(objects_to_delete), 1000):
                    batch = objects_to_delete[i : i + 1000]

                    self.s3_client.delete_objects(
                        Bucket=self.aws_settings.s3_log_bucket,
                        Delete={"Objects": batch},
                    )

                    self.deleted_files += len(batch)

            self.logger.info(f"S3 아카이브 삭제 완료: {self.deleted_files}개")

        except Exception as e:
            self.logger.error(f"S3 아카이브 삭제 실패: {e}")
            raise

    def _manage_log_partitions(self):
        """데이터베이스 로그 테이블 파티션 관리"""
        try:
            from app.core.database_manager_extension import (
                get_extended_database_manager,
            )

            db_manager = get_extended_database_manager()

            # 6개월 이전 파티션 삭제
            cutoff_date = datetime.now() - timedelta(days=180)

            # 시스템 로그 파티션 정리
            query = """
            SELECT schemaname, tablename
            FROM pg_tables
            WHERE tablename LIKE 'system_logs_%'
            AND tablename < %s
            """

            partition_name = f"system_logs_{cutoff_date.strftime('%Y_%m')}"
            old_partitions = db_manager.db_manager.fetch_all(query, (partition_name,))

            for partition in old_partitions:
                drop_query = f"DROP TABLE IF EXISTS {partition['tablename']}"
                db_manager.db_manager.execute_update(drop_query)
                self.logger.info(f"로그 파티션 삭제: {partition['tablename']}")

            # 새 파티션 생성 (다음 3개월)
            for i in range(3):
                future_date = datetime.now() + timedelta(days=30 * i)
                partition_name = f"system_logs_{future_date.strftime('%Y_%m')}"

                create_query = f"""
                CREATE TABLE IF NOT EXISTS {partition_name}
                PARTITION OF system_logs
                FOR VALUES FROM ('{future_date.strftime("%Y-%m-01")}')
                TO ('{(future_date + timedelta(days=32)).strftime("%Y-%m-01")}')
                """

                db_manager.db_manager.execute_update(create_query)
                self.logger.debug(f"로그 파티션 생성: {partition_name}")

        except Exception as e:
            self.logger.error(f"로그 파티션 관리 실패: {e}")
            # 파티션 관리 실패는 전체 작업을 실패시키지 않음

    async def _check_disk_usage(self) -> float:
        """디스크 사용량 확인"""
        try:
            log_dir = Path(self.log_settings.log_directory)
            disk_usage = shutil.disk_usage(log_dir)

            usage_percent = (disk_usage.used / disk_usage.total) * 100

            self.logger.info(
                f"디스크 사용량: {usage_percent:.1f}% "
                f"({disk_usage.used // 1024**3}GB / {disk_usage.total // 1024**3}GB)"
            )

            return usage_percent

        except Exception as e:
            self.logger.error(f"디스크 사용량 확인 실패: {e}")
            return 0.0

    async def _send_disk_usage_alert(self, usage_percent: float):
        """디스크 사용량 경고 알림"""
        try:
            # TODO: 알림 시스템 연동
            alert_message = (
                f"WeatherFlick 배치 시스템 디스크 사용률 경고: {usage_percent:.1f}%"
            )
            self.logger.warning(alert_message)

            # Slack, 이메일 등으로 알림 발송
            # await send_alert("HIGH_DISK_USAGE", alert_message)

        except Exception as e:
            self.logger.error(f"디스크 사용량 알림 발송 실패: {e}")


# 작업 실행 함수
async def log_cleanup_task() -> Dict[str, Any]:
    """로그 정리 작업 실행 함수"""
    job = LogCleanupJob()
    return await job.execute()
