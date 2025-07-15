"""
백업 관리 시스템

API 원본 데이터의 백업과 복구를 관리합니다.
"""

import logging
import gzip
import bz2
import lzma
import json
import hashlib
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from pathlib import Path
from enum import Enum
import asyncio
import aiofiles

from app.archiving.archival_policies import (
    CompressionType, StorageLocation, ArchivalRule, get_archival_policy_manager
)

logger = logging.getLogger(__name__)


class BackupStatus(Enum):
    """백업 상태"""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    CORRUPTED = "corrupted"


@dataclass
class BackupRecord:
    """백업 기록"""
    backup_id: str
    original_data_id: str
    api_provider: str
    endpoint: str
    source_path: Optional[str] = None
    backup_path: str = ""
    compression: CompressionType = CompressionType.NONE
    storage_location: StorageLocation = StorageLocation.LOCAL_DISK
    original_size_bytes: int = 0
    compressed_size_bytes: int = 0
    compression_ratio: float = 0.0
    checksum: str = ""
    status: BackupStatus = BackupStatus.PENDING
    created_at: datetime = field(default_factory=datetime.now)
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class BackupConfiguration:
    """백업 설정"""
    base_backup_path: str = "./data/backups"
    max_concurrent_backups: int = 3
    verify_integrity: bool = True
    auto_cleanup_days: int = 30
    compression_level: int = 6  # 1-9 범위
    enable_deduplication: bool = True
    cloud_storage_config: Dict[str, Any] = field(default_factory=dict)


class CompressionHandler:
    """압축 처리 핸들러"""
    
    @staticmethod
    def get_compressor(compression_type: CompressionType, level: int = 6):
        """압축기 반환"""
        if compression_type == CompressionType.GZIP:
            return lambda data: gzip.compress(data, compresslevel=level)
        elif compression_type == CompressionType.BZIP2:
            return lambda data: bz2.compress(data, compresslevel=level)
        elif compression_type == CompressionType.LZMA:
            return lambda data: lzma.compress(data, preset=level)
        else:
            return lambda data: data
    
    @staticmethod
    def get_decompressor(compression_type: CompressionType):
        """압축 해제기 반환"""
        if compression_type == CompressionType.GZIP:
            return gzip.decompress
        elif compression_type == CompressionType.BZIP2:
            return bz2.decompress
        elif compression_type == CompressionType.LZMA:
            return lzma.decompress
        else:
            return lambda data: data
    
    @staticmethod
    def get_file_extension(compression_type: CompressionType) -> str:
        """파일 확장자 반환"""
        extensions = {
            CompressionType.NONE: "",
            CompressionType.GZIP: ".gz",
            CompressionType.BZIP2: ".bz2",
            CompressionType.LZMA: ".xz"
        }
        return extensions.get(compression_type, "")


class BackupManager:
    """백업 관리자"""
    
    def __init__(self, config: Optional[BackupConfiguration] = None):
        """백업 관리자 초기화"""
        self.config = config or BackupConfiguration()
        self.policy_manager = get_archival_policy_manager()
        self.compression_handler = CompressionHandler()
        
        # 백업 기록 저장소
        self.backup_records: Dict[str, BackupRecord] = {}
        
        # 백업 디렉토리 설정
        self.backup_base_path = Path(self.config.base_backup_path)
        self.backup_base_path.mkdir(parents=True, exist_ok=True)
        
        # 동시 백업 제한용 세마포어
        self.backup_semaphore = asyncio.Semaphore(self.config.max_concurrent_backups)
        
        # 통계
        self.backup_stats = {
            "total_backups": 0,
            "successful_backups": 0,
            "failed_backups": 0,
            "total_original_size_bytes": 0,
            "total_compressed_size_bytes": 0,
            "average_compression_ratio": 0.0
        }
        
        logger.info(f"백업 관리자 초기화 완료 (기본 경로: {self.backup_base_path})")
    
    def generate_backup_id(self, api_provider: str, endpoint: str, data_id: str) -> str:
        """백업 ID 생성"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        hash_input = f"{api_provider}_{endpoint}_{data_id}_{timestamp}"
        hash_value = hashlib.md5(hash_input.encode()).hexdigest()[:8]
        return f"{api_provider}_{endpoint}_{timestamp}_{hash_value}"
    
    def calculate_checksum(self, data: bytes) -> str:
        """체크섬 계산"""
        return hashlib.sha256(data).hexdigest()
    
    def get_backup_path(self, backup_record: BackupRecord) -> Path:
        """백업 파일 경로 생성"""
        # 제공자별 디렉토리 구조: backups/KTO/2024/01/backup_id.json.gz
        date_path = backup_record.created_at.strftime("%Y/%m")
        provider_path = self.backup_base_path / backup_record.api_provider / date_path
        provider_path.mkdir(parents=True, exist_ok=True)
        
        file_extension = self.compression_handler.get_file_extension(backup_record.compression)
        filename = f"{backup_record.backup_id}.json{file_extension}"
        
        return provider_path / filename
    
    async def backup_data(self, data_id: str, api_provider: str, endpoint: str, 
                         data: Dict[str, Any], rule: ArchivalRule) -> BackupRecord:
        """데이터 백업 실행"""
        async with self.backup_semaphore:
            backup_id = self.generate_backup_id(api_provider, endpoint, data_id)
            
            backup_record = BackupRecord(
                backup_id=backup_id,
                original_data_id=data_id,
                api_provider=api_provider,
                endpoint=endpoint,
                compression=rule.compression,
                storage_location=rule.target_location,
                status=BackupStatus.IN_PROGRESS
            )
            
            try:
                # 데이터를 JSON으로 직렬화
                json_data = json.dumps(data, ensure_ascii=False, indent=2)
                original_bytes = json_data.encode('utf-8')
                backup_record.original_size_bytes = len(original_bytes)
                
                # 압축 적용
                compressor = self.compression_handler.get_compressor(
                    rule.compression, self.config.compression_level
                )
                compressed_data = compressor(original_bytes)
                backup_record.compressed_size_bytes = len(compressed_data)
                
                # 압축률 계산
                if backup_record.original_size_bytes > 0:
                    backup_record.compression_ratio = (
                        1 - backup_record.compressed_size_bytes / backup_record.original_size_bytes
                    ) * 100
                
                # 체크섬 계산
                backup_record.checksum = self.calculate_checksum(compressed_data)
                
                # 백업 파일 경로 설정
                backup_path = self.get_backup_path(backup_record)
                backup_record.backup_path = str(backup_path)
                
                # 파일 저장
                if rule.target_location == StorageLocation.LOCAL_DISK:
                    await self._save_to_local_disk(backup_path, compressed_data)
                elif rule.target_location == StorageLocation.CLOUD_STORAGE:
                    await self._save_to_cloud_storage(backup_record, compressed_data)
                elif rule.target_location == StorageLocation.DISTRIBUTED_STORAGE:
                    await self._save_to_distributed_storage(backup_record, compressed_data)
                else:
                    raise ValueError(f"지원하지 않는 저장 위치: {rule.target_location}")
                
                # 무결성 검증
                if self.config.verify_integrity:
                    if not await self._verify_backup_integrity(backup_record):
                        backup_record.status = BackupStatus.CORRUPTED
                        backup_record.error_message = "백업 무결성 검증 실패"
                    else:
                        backup_record.status = BackupStatus.COMPLETED
                else:
                    backup_record.status = BackupStatus.COMPLETED
                
                backup_record.completed_at = datetime.now()
                
                # 통계 업데이트
                self._update_backup_statistics(backup_record)
                
                logger.info(
                    f"백업 완료: {backup_id}, "
                    f"압축률: {backup_record.compression_ratio:.1f}%, "
                    f"크기: {backup_record.compressed_size_bytes:,} bytes"
                )
                
            except Exception as e:
                backup_record.status = BackupStatus.FAILED
                backup_record.error_message = str(e)
                backup_record.completed_at = datetime.now()
                logger.error(f"백업 실패: {backup_id}, 오류: {e}")
            
            # 백업 기록 저장
            self.backup_records[backup_id] = backup_record
            
            return backup_record
    
    async def _save_to_local_disk(self, backup_path: Path, data: bytes):
        """로컬 디스크에 저장"""
        async with aiofiles.open(backup_path, 'wb') as f:
            await f.write(data)
    
    async def _save_to_cloud_storage(self, backup_record: BackupRecord, data: bytes):
        """클라우드 스토리지에 저장 (구현 예정)"""
        # TODO: AWS S3, Google Cloud Storage, Azure Blob Storage 연동
        logger.warning("클라우드 스토리지 백업은 아직 구현되지 않았습니다")
        raise NotImplementedError("클라우드 스토리지 백업 미구현")
    
    async def _save_to_distributed_storage(self, backup_record: BackupRecord, data: bytes):
        """분산 스토리지에 저장 (구현 예정)"""
        # TODO: HDFS, Ceph, GlusterFS 등 분산 스토리지 연동
        logger.warning("분산 스토리지 백업은 아직 구현되지 않았습니다")
        raise NotImplementedError("분산 스토리지 백업 미구현")
    
    async def _verify_backup_integrity(self, backup_record: BackupRecord) -> bool:
        """백업 무결성 검증"""
        try:
            if backup_record.storage_location == StorageLocation.LOCAL_DISK:
                backup_path = Path(backup_record.backup_path)
                if not backup_path.exists():
                    return False
                
                async with aiofiles.open(backup_path, 'rb') as f:
                    stored_data = await f.read()
                
                # 체크섬 검증
                stored_checksum = self.calculate_checksum(stored_data)
                return stored_checksum == backup_record.checksum
            
            # 다른 저장 위치는 구현 예정
            return True
            
        except Exception as e:
            logger.error(f"백업 무결성 검증 오류: {e}")
            return False
    
    def _update_backup_statistics(self, backup_record: BackupRecord):
        """백업 통계 업데이트"""
        self.backup_stats["total_backups"] += 1
        
        if backup_record.status == BackupStatus.COMPLETED:
            self.backup_stats["successful_backups"] += 1
            self.backup_stats["total_original_size_bytes"] += backup_record.original_size_bytes
            self.backup_stats["total_compressed_size_bytes"] += backup_record.compressed_size_bytes
            
            # 평균 압축률 재계산
            if self.backup_stats["total_original_size_bytes"] > 0:
                self.backup_stats["average_compression_ratio"] = (
                    1 - self.backup_stats["total_compressed_size_bytes"] / 
                    self.backup_stats["total_original_size_bytes"]
                ) * 100
        else:
            self.backup_stats["failed_backups"] += 1
    
    async def restore_data(self, backup_id: str) -> Optional[Dict[str, Any]]:
        """백업 데이터 복원"""
        if backup_id not in self.backup_records:
            logger.error(f"백업 기록을 찾을 수 없습니다: {backup_id}")
            return None
        
        backup_record = self.backup_records[backup_id]
        
        try:
            # 백업 파일 읽기
            if backup_record.storage_location == StorageLocation.LOCAL_DISK:
                backup_path = Path(backup_record.backup_path)
                async with aiofiles.open(backup_path, 'rb') as f:
                    compressed_data = await f.read()
            else:
                logger.error(f"지원하지 않는 저장 위치: {backup_record.storage_location}")
                return None
            
            # 무결성 검증
            if self.calculate_checksum(compressed_data) != backup_record.checksum:
                logger.error(f"백업 파일 체크섬 불일치: {backup_id}")
                return None
            
            # 압축 해제
            decompressor = self.compression_handler.get_decompressor(backup_record.compression)
            original_data = decompressor(compressed_data)
            
            # JSON 파싱
            json_data = original_data.decode('utf-8')
            restored_data = json.loads(json_data)
            
            logger.info(f"백업 데이터 복원 완료: {backup_id}")
            return restored_data
            
        except Exception as e:
            logger.error(f"백업 데이터 복원 실패: {backup_id}, 오류: {e}")
            return None
    
    async def cleanup_old_backups(self, cleanup_days: Optional[int] = None) -> int:
        """오래된 백업 정리"""
        cleanup_days = cleanup_days or self.config.auto_cleanup_days
        cutoff_date = datetime.now() - timedelta(days=cleanup_days)
        
        cleaned_count = 0
        backup_ids_to_remove = []
        
        for backup_id, backup_record in self.backup_records.items():
            if backup_record.created_at < cutoff_date:
                try:
                    # 백업 파일 삭제
                    if backup_record.storage_location == StorageLocation.LOCAL_DISK:
                        backup_path = Path(backup_record.backup_path)
                        if backup_path.exists():
                            backup_path.unlink()
                    
                    backup_ids_to_remove.append(backup_id)
                    cleaned_count += 1
                    
                except Exception as e:
                    logger.error(f"백업 파일 삭제 실패: {backup_id}, 오류: {e}")
        
        # 기록에서 제거
        for backup_id in backup_ids_to_remove:
            del self.backup_records[backup_id]
        
        logger.info(f"오래된 백업 정리 완료: {cleaned_count}개 삭제")
        return cleaned_count
    
    def get_backup_statistics(self) -> Dict[str, Any]:
        """백업 통계 반환"""
        return {
            **self.backup_stats,
            "total_backup_records": len(self.backup_records),
            "backup_by_status": {
                status.value: sum(
                    1 for record in self.backup_records.values() 
                    if record.status == status
                )
                for status in BackupStatus
            },
            "backup_by_provider": {
                provider: sum(
                    1 for record in self.backup_records.values()
                    if record.api_provider == provider
                )
                for provider in set(record.api_provider for record in self.backup_records.values())
            },
            "backup_by_compression": {
                compression.value: sum(
                    1 for record in self.backup_records.values()
                    if record.compression == compression
                )
                for compression in CompressionType
            }
        }
    
    def find_backups(self, api_provider: str = None, endpoint: str = None, 
                    status: BackupStatus = None) -> List[BackupRecord]:
        """백업 검색"""
        results = []
        
        for backup_record in self.backup_records.values():
            if api_provider and backup_record.api_provider != api_provider:
                continue
            if endpoint and backup_record.endpoint != endpoint:
                continue
            if status and backup_record.status != status:
                continue
            
            results.append(backup_record)
        
        # 생성 시간 역순으로 정렬
        return sorted(results, key=lambda x: x.created_at, reverse=True)
    
    def get_backup_record(self, backup_id: str) -> Optional[BackupRecord]:
        """백업 기록 반환"""
        return self.backup_records.get(backup_id)


# 전역 백업 관리자 인스턴스
_backup_manager: Optional[BackupManager] = None


def get_backup_manager(config: Optional[BackupConfiguration] = None) -> BackupManager:
    """전역 백업 관리자 인스턴스 반환 (싱글톤)"""
    global _backup_manager
    
    if _backup_manager is None:
        _backup_manager = BackupManager(config)
    
    return _backup_manager


def reset_backup_manager():
    """백업 관리자 인스턴스 재설정 (테스트용)"""
    global _backup_manager
    _backup_manager = None