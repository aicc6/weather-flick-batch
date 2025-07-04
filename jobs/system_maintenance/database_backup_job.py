"""
데이터베이스 백업 자동화 작업

PostgreSQL 데이터베이스의 정기적인 백업을 수행하고 압축하여 저장하는 배치 작업입니다.
"""

import os
import subprocess
import gzip
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List

from app.core.base_job import BaseJob, JobResult, JobConfig
from config.settings import get_database_config, get_app_settings


class DatabaseBackupJob(BaseJob):
    """데이터베이스 백업 자동화 배치 작업"""

    def __init__(self, config: JobConfig):
        super().__init__(config)
        self.db_config = get_database_config()
        self.app_settings = get_app_settings()
        self.backup_dir = Path("data/backups")
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        self.processed_records = 0

    def execute(self) -> JobResult:
        """데이터베이스 백업 실행"""
        result = JobResult(
            job_name=self.config.job_name,
            job_type=self.config.job_type,
            status="running",
            start_time=datetime.now(),
        )

        try:
            backup_files = []
            total_size = 0

            # 1. 전체 데이터베이스 백업
            full_backup_file = self._create_full_backup()
            if full_backup_file:
                backup_files.append(full_backup_file)
                total_size += full_backup_file["size"]
                self.logger.info(f"전체 DB 백업 완료: {full_backup_file['filename']}")

            # 2. 테이블별 백업 (중요 테이블만)
            table_backups = self._create_table_backups()
            backup_files.extend(table_backups)
            total_size += sum(backup["size"] for backup in table_backups)
            self.logger.info(f"테이블별 백업 {len(table_backups)}개 완료")

            # 3. 백업 파일 압축
            compressed_files = self._compress_backup_files(backup_files)
            self.logger.info(f"백업 파일 {len(compressed_files)}개 압축 완료")

            # 4. 오래된 백업 파일 정리
            cleaned_files = self._cleanup_old_backups()
            self.logger.info(f"오래된 백업 파일 {cleaned_files}개 정리")

            # 5. 백업 검증
            verification_results = self._verify_backups(compressed_files)

            result.processed_records = len(backup_files)
            result.metadata = {
                "backup_files_created": len(backup_files),
                "total_backup_size_mb": round(total_size / (1024 * 1024), 2),
                "compressed_files": len(compressed_files),
                "cleaned_old_files": cleaned_files,
                "verification_passed": all(verification_results.values()),
                "backup_directory": str(self.backup_dir),
            }

            self.logger.info(
                f"데이터베이스 백업 완료: {len(backup_files)}개 파일, {total_size / (1024 * 1024):.1f}MB"
            )

        except Exception as e:
            self.logger.error(f"데이터베이스 백업 실패: {str(e)}")
            raise

        return result

    def _create_full_backup(self) -> Dict:
        """전체 데이터베이스 백업 생성"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_filename = f"full_backup_{timestamp}.sql"
        backup_path = self.backup_dir / backup_filename

        try:
            # pg_dump을 사용한 전체 백업
            env = os.environ.copy()
            env["PGPASSWORD"] = self.db_config.password

            cmd = [
                "pg_dump",
                "-h",
                self.db_config.host,
                "-p",
                str(self.db_config.port),
                "-U",
                self.db_config.user,
                "-d",
                self.db_config.database,
                "--clean",
                "--create",
                "--if-exists",
                "--verbose",
                "-f",
                str(backup_path),
            ]

            self.logger.debug(
                f"백업 명령 실행: {' '.join(cmd[:-2])}"
            )  # 패스워드 제외하고 로깅

            result = subprocess.run(
                cmd,
                env=env,
                capture_output=True,
                text=True,
                timeout=3600,  # 1시간 타임아웃
            )

            if result.returncode != 0:
                self.logger.error(f"pg_dump 실패: {result.stderr}")
                return None

            file_size = backup_path.stat().st_size

            return {
                "filename": backup_filename,
                "path": str(backup_path),
                "size": file_size,
                "type": "full",
                "created_at": datetime.now(),
            }

        except subprocess.TimeoutExpired:
            self.logger.error("백업 타임아웃 발생")
        except FileNotFoundError:
            self.logger.error(
                "pg_dump를 찾을 수 없음. PostgreSQL 클라이언트가 설치되지 않았습니다."
            )
        except Exception as e:
            self.logger.error(f"전체 백업 생성 실패: {str(e)}")

        return None

    def _create_table_backups(self) -> List[Dict]:
        """중요 테이블별 백업 생성"""
        important_tables = [
            "historical_weather_daily",
            "weather_forecast",
            "tourist_attractions",
            "travel_weather_scores",
            "regions",
        ]

        table_backups = []
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        for table_name in important_tables:
            try:
                backup_filename = f"table_{table_name}_{timestamp}.sql"
                backup_path = self.backup_dir / backup_filename

                env = os.environ.copy()
                env["PGPASSWORD"] = self.db_config.password

                cmd = [
                    "pg_dump",
                    "-h",
                    self.db_config.host,
                    "-p",
                    str(self.db_config.port),
                    "-U",
                    self.db_config.user,
                    "-d",
                    self.db_config.database,
                    "--table",
                    table_name,
                    "--data-only",
                    "--inserts",
                    "-f",
                    str(backup_path),
                ]

                result = subprocess.run(
                    cmd,
                    env=env,
                    capture_output=True,
                    text=True,
                    timeout=1800,  # 30분 타임아웃
                )

                if result.returncode == 0 and backup_path.exists():
                    file_size = backup_path.stat().st_size
                    table_backups.append(
                        {
                            "filename": backup_filename,
                            "path": str(backup_path),
                            "size": file_size,
                            "type": "table",
                            "table_name": table_name,
                            "created_at": datetime.now(),
                        }
                    )
                    self.logger.debug(f"테이블 {table_name} 백업 완료")
                else:
                    self.logger.warning(
                        f"테이블 {table_name} 백업 실패: {result.stderr}"
                    )

            except Exception as e:
                self.logger.warning(f"테이블 {table_name} 백업 실패: {str(e)}")

        return table_backups

    def _compress_backup_files(self, backup_files: List[Dict]) -> List[Dict]:
        """백업 파일 압축"""
        compressed_files = []

        for backup_file in backup_files:
            try:
                source_path = Path(backup_file["path"])
                if not source_path.exists():
                    continue

                compressed_filename = f"{backup_file['filename']}.gz"
                compressed_path = self.backup_dir / compressed_filename

                # gzip 압축
                with open(source_path, "rb") as f_in:
                    with gzip.open(compressed_path, "wb") as f_out:
                        shutil.copyfileobj(f_in, f_out)

                # 압축 정보 저장
                compressed_size = compressed_path.stat().st_size
                compression_ratio = (1 - compressed_size / backup_file["size"]) * 100

                compressed_files.append(
                    {
                        "filename": compressed_filename,
                        "path": str(compressed_path),
                        "size": compressed_size,
                        "original_size": backup_file["size"],
                        "compression_ratio": round(compression_ratio, 1),
                        "type": backup_file["type"],
                        "created_at": datetime.now(),
                    }
                )

                # 원본 파일 삭제
                source_path.unlink()
                self.logger.debug(
                    f"백업 파일 압축 완료: {compressed_filename} ({compression_ratio:.1f}% 압축)"
                )

            except Exception as e:
                self.logger.warning(
                    f"백업 파일 압축 실패 [{backup_file['filename']}]: {str(e)}"
                )

        return compressed_files

    def _cleanup_old_backups(self, retention_days: int = 30) -> int:
        """오래된 백업 파일 정리"""
        cutoff_date = datetime.now() - timedelta(days=retention_days)
        cleaned_count = 0

        try:
            for backup_file in self.backup_dir.glob("*.gz"):
                file_mtime = datetime.fromtimestamp(backup_file.stat().st_mtime)

                if file_mtime < cutoff_date:
                    backup_file.unlink()
                    cleaned_count += 1
                    self.logger.debug(f"오래된 백업 파일 삭제: {backup_file.name}")

        except Exception as e:
            self.logger.warning(f"백업 파일 정리 실패: {str(e)}")

        return cleaned_count

    def _verify_backups(self, backup_files: List[Dict]) -> Dict[str, bool]:
        """백업 파일 검증"""
        verification_results = {}

        for backup_file in backup_files:
            try:
                backup_path = Path(backup_file["path"])

                # 파일 존재 확인
                if not backup_path.exists():
                    verification_results[backup_file["filename"]] = False
                    continue

                # 파일 크기 확인 (너무 작으면 실패)
                min_size = 100  # 100 bytes
                if backup_path.stat().st_size < min_size:
                    verification_results[backup_file["filename"]] = False
                    continue

                # gzip 파일 무결성 검사
                try:
                    with gzip.open(backup_path, "rb") as f:
                        # 첫 1KB 읽어보기
                        f.read(1024)
                    verification_results[backup_file["filename"]] = True
                except:
                    verification_results[backup_file["filename"]] = False

            except Exception as e:
                self.logger.warning(
                    f"백업 파일 검증 실패 [{backup_file['filename']}]: {str(e)}"
                )
                verification_results[backup_file["filename"]] = False

        return verification_results

    def _get_backup_statistics(self) -> Dict:
        """백업 통계 정보 조회"""
        try:
            backup_files = list(self.backup_dir.glob("*.gz"))
            total_size = sum(f.stat().st_size for f in backup_files)

            # 파일 타입별 분류
            full_backups = [f for f in backup_files if "full_backup" in f.name]
            table_backups = [f for f in backup_files if "table_" in f.name]

            return {
                "total_files": len(backup_files),
                "total_size_mb": round(total_size / (1024 * 1024), 2),
                "full_backups": len(full_backups),
                "table_backups": len(table_backups),
                "oldest_backup": min(
                    (f.stat().st_mtime for f in backup_files), default=0
                ),
                "newest_backup": max(
                    (f.stat().st_mtime for f in backup_files), default=0
                ),
            }
        except Exception as e:
            self.logger.warning(f"백업 통계 조회 실패: {str(e)}")
            return {}

    def pre_execute(self) -> bool:
        """실행 전 검증"""
        # pg_dump 명령어 존재 확인
        try:
            result = subprocess.run(
                ["pg_dump", "--version"], capture_output=True, text=True, timeout=10
            )
            if result.returncode != 0:
                self.logger.error("pg_dump 명령어를 실행할 수 없습니다")
                return False
        except (FileNotFoundError, subprocess.TimeoutExpired):
            self.logger.error(
                "pg_dump를 찾을 수 없습니다. PostgreSQL 클라이언트 설치가 필요합니다"
            )
            return False

        # 백업 디렉토리 쓰기 권한 확인
        try:
            test_file = self.backup_dir / "test_write.tmp"
            test_file.write_text("test")
            test_file.unlink()
        except Exception as e:
            self.logger.error(f"백업 디렉토리 쓰기 권한 없음: {str(e)}")
            return False

        # 디스크 공간 확인 (최소 1GB)
        try:
            disk_usage = shutil.disk_usage(self.backup_dir)
            free_space_gb = disk_usage.free / (1024**3)
            if free_space_gb < 1.0:
                self.logger.warning(f"디스크 여유 공간 부족: {free_space_gb:.1f}GB")
                # 경고만 하고 계속 진행
        except Exception as e:
            self.logger.warning(f"디스크 공간 확인 실패: {str(e)}")

        return True

    def post_execute(self, result: JobResult) -> None:
        """실행 후 처리"""
        super().post_execute(result)

        # 백업 통계 로깅
        stats = self._get_backup_statistics()
        if stats:
            self.logger.info(
                f"백업 디렉토리 통계: 총 {stats['total_files']}개 파일, "
                f"{stats['total_size_mb']:.1f}MB"
            )

        # 백업 성공률 로깅
        if result.metadata and "verification_passed" in result.metadata:
            verification_status = (
                "성공" if result.metadata["verification_passed"] else "실패"
            )
            self.logger.info(f"백업 검증: {verification_status}")

        # 디스크 사용량 경고
        try:
            disk_usage = shutil.disk_usage(self.backup_dir)
            used_percent = (disk_usage.used / disk_usage.total) * 100
            if used_percent > 90:
                self.logger.warning(f"디스크 사용률 높음: {used_percent:.1f}%")
        except Exception:
            pass

