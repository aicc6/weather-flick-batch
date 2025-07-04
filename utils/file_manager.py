"""
파일 관리 유틸리티 모듈

파일 저장, 로드, 압축 등의 기능을 제공합니다.
"""

import json
import csv
import gzip
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Union
import logging

from config.constants import DATA_PATHS, DATE_FORMATS


class FileManager:
    """파일 관리 클래스"""

    def __init__(self, base_path: str = "."):
        self.base_path = Path(base_path)
        self.logger = logging.getLogger(__name__)
        self._ensure_directories()

    def _ensure_directories(self) -> None:
        """필요한 디렉토리 생성"""
        for path_key, path_value in DATA_PATHS.items():
            dir_path = self.base_path / path_value
            dir_path.mkdir(parents=True, exist_ok=True)

    def save_json(
        self,
        data: Union[Dict, List],
        filename: str,
        directory: str = "processed_data",
        compress: bool = False,
    ) -> Path:
        """JSON 데이터 저장"""
        try:
            dir_path = self.base_path / DATA_PATHS[directory]
            timestamp = datetime.now().strftime(DATE_FORMATS["file_datetime"])

            if not filename.endswith(".json"):
                filename = f"{filename}_{timestamp}.json"

            file_path = dir_path / filename

            if compress:
                file_path = file_path.with_suffix(".json.gz")
                with gzip.open(file_path, "wt", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2, default=str)
            else:
                with open(file_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2, default=str)

            self.logger.info(f"JSON 파일 저장 완료: {file_path}")
            return file_path

        except Exception as e:
            self.logger.error(f"JSON 파일 저장 실패: {e}")
            raise

    def load_json(self, file_path: Union[str, Path]) -> Union[Dict, List]:
        """JSON 데이터 로드"""
        try:
            file_path = Path(file_path)

            if file_path.suffix == ".gz":
                with gzip.open(file_path, "rt", encoding="utf-8") as f:
                    data = json.load(f)
            else:
                with open(file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)

            self.logger.debug(f"JSON 파일 로드 완료: {file_path}")
            return data

        except Exception as e:
            self.logger.error(f"JSON 파일 로드 실패: {e}")
            raise

    def save_csv(
        self, data: List[Dict], filename: str, directory: str = "processed_data"
    ) -> Path:
        """CSV 데이터 저장"""
        try:
            if not data:
                raise ValueError("저장할 데이터가 없습니다")

            dir_path = self.base_path / DATA_PATHS[directory]
            timestamp = datetime.now().strftime(DATE_FORMATS["file_datetime"])

            if not filename.endswith(".csv"):
                filename = f"{filename}_{timestamp}.csv"

            file_path = dir_path / filename

            fieldnames = data[0].keys()
            with open(file_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(data)

            self.logger.info(f"CSV 파일 저장 완료: {file_path}")
            return file_path

        except Exception as e:
            self.logger.error(f"CSV 파일 저장 실패: {e}")
            raise

    def load_csv(self, file_path: Union[str, Path]) -> List[Dict]:
        """CSV 데이터 로드"""
        try:
            file_path = Path(file_path)
            data = []

            with open(file_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                data = list(reader)

            self.logger.debug(f"CSV 파일 로드 완료: {file_path}, {len(data)}건")
            return data

        except Exception as e:
            self.logger.error(f"CSV 파일 로드 실패: {e}")
            raise

    def backup_file(
        self, source_path: Union[str, Path], backup_directory: str = "raw_data"
    ) -> Path:
        """파일 백업"""
        try:
            source_path = Path(source_path)
            backup_dir = self.base_path / DATA_PATHS[backup_directory] / "backups"
            backup_dir.mkdir(exist_ok=True)

            timestamp = datetime.now().strftime(DATE_FORMATS["file_datetime"])
            backup_filename = f"{source_path.stem}_{timestamp}{source_path.suffix}"
            backup_path = backup_dir / backup_filename

            shutil.copy2(source_path, backup_path)

            self.logger.info(f"파일 백업 완료: {source_path} -> {backup_path}")
            return backup_path

        except Exception as e:
            self.logger.error(f"파일 백업 실패: {e}")
            raise

    def compress_old_files(self, directory: str, days_old: int = 30) -> int:
        """오래된 파일 압축"""
        try:
            dir_path = self.base_path / DATA_PATHS[directory]
            if not dir_path.exists():
                return 0

            cutoff_date = datetime.now().timestamp() - (days_old * 24 * 60 * 60)
            compressed_count = 0

            for file_path in dir_path.rglob("*.json"):
                if file_path.suffix == ".gz":
                    continue

                if file_path.stat().st_mtime < cutoff_date:
                    self._compress_file(file_path)
                    compressed_count += 1

            self.logger.info(f"오래된 파일 {compressed_count}개 압축 완료")
            return compressed_count

        except Exception as e:
            self.logger.error(f"파일 압축 실패: {e}")
            raise

    def _compress_file(self, file_path: Path) -> None:
        """개별 파일 압축"""
        compressed_path = file_path.with_suffix(file_path.suffix + ".gz")

        with open(file_path, "rb") as f_in:
            with gzip.open(compressed_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

        file_path.unlink()  # 원본 파일 삭제
        self.logger.debug(f"파일 압축: {file_path} -> {compressed_path}")

    def cleanup_old_files(self, directory: str, days_old: int = 90) -> int:
        """오래된 파일 정리"""
        try:
            dir_path = self.base_path / DATA_PATHS[directory]
            if not dir_path.exists():
                return 0

            cutoff_date = datetime.now().timestamp() - (days_old * 24 * 60 * 60)
            deleted_count = 0

            for file_path in dir_path.rglob("*"):
                if file_path.is_file() and file_path.stat().st_mtime < cutoff_date:
                    file_path.unlink()
                    deleted_count += 1
                    self.logger.debug(f"파일 삭제: {file_path}")

            self.logger.info(f"오래된 파일 {deleted_count}개 삭제 완료")
            return deleted_count

        except Exception as e:
            self.logger.error(f"파일 정리 실패: {e}")
            raise

    def get_file_info(self, file_path: Union[str, Path]) -> Dict[str, Any]:
        """파일 정보 조회"""
        try:
            file_path = Path(file_path)
            stat = file_path.stat()

            return {
                "name": file_path.name,
                "size": stat.st_size,
                "size_mb": round(stat.st_size / (1024 * 1024), 2),
                "created": datetime.fromtimestamp(stat.st_ctime),
                "modified": datetime.fromtimestamp(stat.st_mtime),
                "extension": file_path.suffix,
                "exists": file_path.exists(),
                "is_compressed": file_path.suffix == ".gz",
            }

        except Exception as e:
            self.logger.error(f"파일 정보 조회 실패: {e}")
            return {}

    def list_files(self, directory: str, pattern: str = "*") -> List[Dict[str, Any]]:
        """디렉토리 내 파일 목록 조회"""
        try:
            dir_path = self.base_path / DATA_PATHS[directory]
            if not dir_path.exists():
                return []

            files = []
            for file_path in dir_path.glob(pattern):
                if file_path.is_file():
                    files.append(self.get_file_info(file_path))

            # 수정 시간 순으로 정렬
            files.sort(key=lambda x: x.get("modified", datetime.min), reverse=True)

            return files

        except Exception as e:
            self.logger.error(f"파일 목록 조회 실패: {e}")
            return []


# 전역 파일 매니저 인스턴스
_file_manager = None


def get_file_manager() -> FileManager:
    """전역 파일 매니저 반환"""
    global _file_manager
    if _file_manager is None:
        _file_manager = FileManager()
    return _file_manager
