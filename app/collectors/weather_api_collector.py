"""
날씨 API 수집기

기상청 API를 통한 날씨 데이터 수집을 담당하는 통합 API 수집기입니다.
기존 WeatherDataCollector를 확장하여 통합 테스트에서 요구하는 인터페이스를 제공합니다.
"""

import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

from app.collectors.weather_collector import WeatherDataCollector
from app.core.multi_api_key_manager import get_api_key_manager, APIProvider
from app.core.unified_api_client import UnifiedAPIClient
from app.core.logger import get_logger


@dataclass
class WeatherCollectionResult:
    """날씨 수집 결과"""
    success: bool
    data: Optional[Dict[str, Any]]
    error: Optional[str]
    processing_time: float
    source: str


class WeatherAPICollector:
    """
    날씨 API 통합 수집기
    
    기상청 API를 통한 모든 날씨 데이터 수집을 담당합니다.
    통합 테스트 및 모니터링 시스템에서 요구하는 표준 인터페이스를 제공합니다.
    """
    
    def __init__(self):
        """날씨 API 수집기 초기화"""
        self.logger = get_logger(__name__)
        self.weather_collector = WeatherDataCollector()
        self.api_client = UnifiedAPIClient()
        
        # 다중 API 키 관리자
        self.key_manager = get_api_key_manager()
        
        # 수집 통계
        self.collection_stats = {
            "total_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "last_collection_time": None,
            "average_response_time": 0.0
        }
        
        self.logger.info("날씨 API 수집기 초기화 완료")
    
    def collect_current_weather(self, region_name: str) -> WeatherCollectionResult:
        """
        현재 날씨 수집
        
        Args:
            region_name: 지역명
            
        Returns:
            WeatherCollectionResult: 수집 결과
        """
        start_time = datetime.now()
        
        try:
            self.collection_stats["total_requests"] += 1
            
            # 기존 날씨 수집기 사용
            weather_data = self.weather_collector.get_current_weather(region_name)
            
            processing_time = (datetime.now() - start_time).total_seconds()
            
            if weather_data:
                self.collection_stats["successful_requests"] += 1
                self.collection_stats["last_collection_time"] = datetime.now()
                self._update_average_response_time(processing_time)
                
                self.logger.info(f"현재 날씨 수집 성공: {region_name} ({processing_time:.3f}초)")
                
                return WeatherCollectionResult(
                    success=True,
                    data=weather_data,
                    error=None,
                    processing_time=processing_time,
                    source="KMA_current"
                )
            else:
                self.collection_stats["failed_requests"] += 1
                error_msg = f"현재 날씨 데이터 수집 실패: {region_name}"
                self.logger.warning(error_msg)
                
                return WeatherCollectionResult(
                    success=False,
                    data=None,
                    error=error_msg,
                    processing_time=processing_time,
                    source="KMA_current"
                )
                
        except Exception as e:
            self.collection_stats["failed_requests"] += 1
            processing_time = (datetime.now() - start_time).total_seconds()
            error_msg = f"현재 날씨 수집 중 오류 발생: {e}"
            self.logger.error(error_msg)
            
            return WeatherCollectionResult(
                success=False,
                data=None,
                error=error_msg,
                processing_time=processing_time,
                source="KMA_current"
            )
    
    def collect_weather_forecast(self, region_name: str, days: int = 3) -> WeatherCollectionResult:
        """
        날씨 예보 수집
        
        Args:
            region_name: 지역명
            days: 예보 일수
            
        Returns:
            WeatherCollectionResult: 수집 결과
        """
        start_time = datetime.now()
        
        try:
            self.collection_stats["total_requests"] += 1
            
            # 기존 날씨 수집기 사용
            forecast_data = self.weather_collector.get_weather_forecast(region_name, days)
            
            processing_time = (datetime.now() - start_time).total_seconds()
            
            if forecast_data:
                self.collection_stats["successful_requests"] += 1
                self.collection_stats["last_collection_time"] = datetime.now()
                self._update_average_response_time(processing_time)
                
                self.logger.info(f"날씨 예보 수집 성공: {region_name}, {len(forecast_data)}일 ({processing_time:.3f}초)")
                
                return WeatherCollectionResult(
                    success=True,
                    data={
                        "region_name": region_name,
                        "forecast_days": days,
                        "forecasts": forecast_data,
                        "count": len(forecast_data)
                    },
                    error=None,
                    processing_time=processing_time,
                    source="KMA_forecast"
                )
            else:
                self.collection_stats["failed_requests"] += 1
                error_msg = f"날씨 예보 데이터 수집 실패: {region_name}"
                self.logger.warning(error_msg)
                
                return WeatherCollectionResult(
                    success=False,
                    data=None,
                    error=error_msg,
                    processing_time=processing_time,
                    source="KMA_forecast"
                )
                
        except Exception as e:
            self.collection_stats["failed_requests"] += 1
            processing_time = (datetime.now() - start_time).total_seconds()
            error_msg = f"날씨 예보 수집 중 오류 발생: {e}"
            self.logger.error(error_msg)
            
            return WeatherCollectionResult(
                success=False,
                data=None,
                error=error_msg,
                processing_time=processing_time,
                source="KMA_forecast"
            )
    
    def collect_historical_weather(self, region_name: str, start_date: str, end_date: str) -> WeatherCollectionResult:
        """
        과거 날씨 데이터 수집
        
        Args:
            region_name: 지역명
            start_date: 시작일 (YYYYMMDD)
            end_date: 종료일 (YYYYMMDD)
            
        Returns:
            WeatherCollectionResult: 수집 결과
        """
        start_time = datetime.now()
        
        try:
            self.collection_stats["total_requests"] += 1
            
            # 기존 날씨 수집기 사용
            historical_data = self.weather_collector.get_historical_weather(region_name, start_date, end_date)
            
            processing_time = (datetime.now() - start_time).total_seconds()
            
            if historical_data:
                self.collection_stats["successful_requests"] += 1
                self.collection_stats["last_collection_time"] = datetime.now()
                self._update_average_response_time(processing_time)
                
                self.logger.info(f"과거 날씨 수집 성공: {region_name}, {len(historical_data)}일 ({processing_time:.3f}초)")
                
                return WeatherCollectionResult(
                    success=True,
                    data={
                        "region_name": region_name,
                        "start_date": start_date,
                        "end_date": end_date,
                        "historical_data": historical_data,
                        "count": len(historical_data)
                    },
                    error=None,
                    processing_time=processing_time,
                    source="KMA_historical"
                )
            else:
                self.collection_stats["failed_requests"] += 1
                error_msg = f"과거 날씨 데이터 수집 실패: {region_name}"
                self.logger.warning(error_msg)
                
                return WeatherCollectionResult(
                    success=False,
                    data=None,
                    error=error_msg,
                    processing_time=processing_time,
                    source="KMA_historical"
                )
                
        except Exception as e:
            self.collection_stats["failed_requests"] += 1
            processing_time = (datetime.now() - start_time).total_seconds()
            error_msg = f"과거 날씨 수집 중 오류 발생: {e}"
            self.logger.error(error_msg)
            
            return WeatherCollectionResult(
                success=False,
                data=None,
                error=error_msg,
                processing_time=processing_time,
                source="KMA_historical"
            )
    
    async def collect_weather_batch(self, regions: List[str], weather_type: str = "current") -> List[WeatherCollectionResult]:
        """
        배치 날씨 데이터 수집
        
        Args:
            regions: 지역 목록
            weather_type: 날씨 타입 ("current", "forecast", "historical")
            
        Returns:
            List[WeatherCollectionResult]: 수집 결과 목록
        """
        results = []
        
        for region in regions:
            try:
                if weather_type == "current":
                    result = self.collect_current_weather(region)
                elif weather_type == "forecast":
                    result = self.collect_weather_forecast(region)
                elif weather_type == "historical":
                    # 최근 7일간 데이터 수집
                    end_date = datetime.now().strftime("%Y%m%d")
                    start_date = (datetime.now() - timedelta(days=7)).strftime("%Y%m%d")
                    result = self.collect_historical_weather(region, start_date, end_date)
                else:
                    result = WeatherCollectionResult(
                        success=False,
                        data=None,
                        error=f"지원하지 않는 날씨 타입: {weather_type}",
                        processing_time=0.0,
                        source="KMA_batch"
                    )
                
                results.append(result)
                
                # API 호출 제한을 위한 대기
                await asyncio.sleep(0.1)
                
            except Exception as e:
                self.logger.error(f"배치 수집 중 오류 ({region}): {e}")
                results.append(WeatherCollectionResult(
                    success=False,
                    data=None,
                    error=str(e),
                    processing_time=0.0,
                    source="KMA_batch"
                ))
        
        return results
    
    def get_api_status(self) -> Dict[str, Any]:
        """
        API 상태 정보 반환
        
        Returns:
            Dict[str, Any]: API 상태 정보
        """
        # API 키 상태 확인
        kma_key = self.key_manager.get_active_key(APIProvider.KMA)
        
        return {
            "api_provider": "KMA",
            "active_key_available": kma_key is not None,
            "key_usage": kma_key.current_usage if kma_key else 0,
            "key_limit": kma_key.daily_limit if kma_key else 0,
            "collection_stats": self.collection_stats.copy(),
            "last_check": datetime.now().isoformat()
        }
    
    def get_collection_statistics(self) -> Dict[str, Any]:
        """
        수집 통계 반환
        
        Returns:
            Dict[str, Any]: 수집 통계
        """
        total_requests = self.collection_stats["total_requests"]
        success_rate = 0.0
        
        if total_requests > 0:
            success_rate = (self.collection_stats["successful_requests"] / total_requests) * 100
        
        return {
            **self.collection_stats,
            "success_rate": success_rate,
            "failure_rate": 100 - success_rate,
            "status": "healthy" if success_rate >= 80 else "warning" if success_rate >= 50 else "critical"
        }
    
    def _update_average_response_time(self, new_time: float):
        """평균 응답 시간 업데이트"""
        current_avg = self.collection_stats["average_response_time"]
        successful = self.collection_stats["successful_requests"]
        
        if successful == 1:
            self.collection_stats["average_response_time"] = new_time
        else:
            # 이동 평균 계산
            self.collection_stats["average_response_time"] = (
                (current_avg * (successful - 1) + new_time) / successful
            )
    
    def reset_statistics(self):
        """통계 초기화"""
        self.collection_stats = {
            "total_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "last_collection_time": None,
            "average_response_time": 0.0
        }
        self.logger.info("날씨 API 수집 통계가 초기화되었습니다")


# 전역 인스턴스
_weather_api_collector: Optional[WeatherAPICollector] = None


def get_weather_api_collector() -> WeatherAPICollector:
    """날씨 API 수집기 싱글톤 인스턴스 반환"""
    global _weather_api_collector
    
    if _weather_api_collector is None:
        _weather_api_collector = WeatherAPICollector()
    
    return _weather_api_collector


def reset_weather_api_collector():
    """날씨 API 수집기 인스턴스 재설정 (테스트용)"""
    global _weather_api_collector
    _weather_api_collector = None