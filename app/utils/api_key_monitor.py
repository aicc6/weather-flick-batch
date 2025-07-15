"""
API 키 상태 모니터링 및 관리 유틸리티

KTO API 키의 상태를 확인하고 문제가 있는 키를 관리하는 도구입니다.
"""

import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List

from app.core.multi_api_key_manager import get_api_key_manager, APIProvider
from app.core.unified_api_client import get_unified_api_client


class APIKeyMonitor:
    """API 키 상태 모니터링 및 관리 클래스"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.key_manager = get_api_key_manager()
        self.api_client = get_unified_api_client()

    async def check_all_keys_health(self, provider: APIProvider) -> Dict:
        """모든 키의 건강 상태 점검"""
        self.logger.info(f"🔍 {provider.value} API 키 건강 상태 점검 시작")
        
        health_results = {
            "provider": provider.value,
            "checked_at": datetime.now().isoformat(),
            "total_keys": 0,
            "healthy_keys": 0,
            "unhealthy_keys": 0,
            "key_results": [],
            "recommendations": []
        }

        detailed_status = self.key_manager.get_detailed_key_status(provider)
        if "error" in detailed_status:
            health_results["error"] = detailed_status["error"]
            return health_results

        health_results["total_keys"] = detailed_status["total_keys"]
        
        async with self.api_client:
            for key_info in detailed_status["keys"]:
                key_result = await self._test_single_key(provider, key_info)
                health_results["key_results"].append(key_result)
                
                if key_result["is_healthy"]:
                    health_results["healthy_keys"] += 1
                else:
                    health_results["unhealthy_keys"] += 1
                
                # API 호출 간격 조정
                await asyncio.sleep(0.5)

        # 권장사항 생성
        health_results["recommendations"] = self._generate_recommendations(health_results)
        
        self.logger.info(
            f"✅ {provider.value} API 키 건강 상태 점검 완료: "
            f"건강한 키 {health_results['healthy_keys']}/{health_results['total_keys']}개"
        )
        
        return health_results

    async def _test_single_key(self, provider: APIProvider, key_info: Dict) -> Dict:
        """개별 키 상태 테스트"""
        key_preview = key_info["key_preview"]
        self.logger.debug(f"🧪 {provider.value} 키 #{key_info['index']} 테스트: {key_preview}")
        
        test_result = {
            "index": key_info["index"],
            "key_preview": key_preview,
            "is_healthy": False,
            "test_timestamp": datetime.now().isoformat(),
            "response_time_ms": None,
            "error": None,
            "status_before": {
                "is_active": key_info["is_active"],
                "is_available": key_info["is_available"],
                "error_count": key_info["error_count"],
                "usage_percent": key_info["usage_percent"]
            }
        }

        # 비활성화된 키는 테스트하지 않음
        if not key_info["is_active"]:
            test_result["error"] = "키가 비활성화됨"
            test_result["is_healthy"] = False
            return test_result

        # 사용 불가능한 키도 건너뜀 (한도 초과, Rate limit 등)
        if not key_info["is_available"]:
            test_result["error"] = key_info["unavailable_reason"]
            test_result["is_healthy"] = False
            return test_result

        try:
            # 간단한 API 호출로 키 유효성 검증
            start_time = datetime.now()
            
            if provider == APIProvider.KTO:
                # KTO API 테스트 - 지역 코드 조회 (작은 데이터)
                response = await self.api_client.call_api(
                    api_provider=provider,
                    endpoint="areaCode2",
                    params={
                        "MobileOS": "ETC",
                        "MobileApp": "WeatherFlick",
                        "_type": "json",
                        "numOfRows": 1,
                        "pageNo": 1
                    },
                    store_raw=False,  # 테스트용이므로 저장하지 않음
                    use_cache=False   # 실제 키 상태 확인을 위해 캐시 사용 안함
                )
            elif provider == APIProvider.KMA:
                # KMA API 테스트 - 간단한 날씨 조회
                response = await self.api_client.call_api(
                    api_provider=provider,
                    endpoint="getUltraSrtNcst",
                    params={
                        "base_date": datetime.now().strftime("%Y%m%d"),
                        "base_time": "0600",
                        "nx": "60",  # 서울
                        "ny": "127",
                        "numOfRows": 1,
                        "pageNo": 1,
                        "dataType": "JSON"
                    },
                    store_raw=False,
                    use_cache=False
                )
            else:
                test_result["error"] = f"지원하지 않는 API 제공자: {provider.value}"
                return test_result

            end_time = datetime.now()
            test_result["response_time_ms"] = int((end_time - start_time).total_seconds() * 1000)

            if response.success:
                test_result["is_healthy"] = True
                self.logger.debug(
                    f"✅ {provider.value} 키 #{key_info['index']} 테스트 성공: {key_preview} "
                    f"({test_result['response_time_ms']}ms)"
                )
            else:
                test_result["is_healthy"] = False
                test_result["error"] = response.error or "알 수 없는 오류"
                self.logger.warning(
                    f"❌ {provider.value} 키 #{key_info['index']} 테스트 실패: {key_preview} "
                    f"- {test_result['error']}"
                )

        except Exception as e:
            test_result["is_healthy"] = False
            test_result["error"] = str(e)
            self.logger.warning(
                f"🔥 {provider.value} 키 #{key_info['index']} 테스트 예외: {key_preview} - {e}"
            )

        return test_result

    def _generate_recommendations(self, health_results: Dict) -> List[str]:
        """건강 상태 기반 권장사항 생성"""
        recommendations = []
        
        total_keys = health_results["total_keys"]
        healthy_keys = health_results["healthy_keys"]
        unhealthy_keys = health_results["unhealthy_keys"]
        
        if total_keys == 0:
            recommendations.append("⚠️ API 키가 설정되지 않았습니다. .env 파일을 확인하세요.")
            return recommendations
        
        if healthy_keys == 0:
            recommendations.append("🚨 모든 API 키가 문제가 있습니다. 긴급 점검이 필요합니다.")
            recommendations.append("💡 새로운 API 키를 발급받거나 기존 키의 한도를 확인하세요.")
        elif healthy_keys < total_keys * 0.5:
            recommendations.append("⚠️ 절반 이상의 API 키에 문제가 있습니다.")
            recommendations.append("💡 문제가 있는 키들을 점검하고 필요시 새 키를 추가하세요.")
        elif unhealthy_keys > 0:
            recommendations.append(f"💡 {unhealthy_keys}개 키에 문제가 있습니다. 개별 점검을 권장합니다.")
        
        # 개별 키 문제 분석
        error_types = {}
        for key_result in health_results["key_results"]:
            if not key_result["is_healthy"] and key_result["error"]:
                error = key_result["error"]
                error_types[error] = error_types.get(error, 0) + 1
        
        for error, count in error_types.items():
            if count > 1:
                recommendations.append(f"📋 공통 문제 ({count}개 키): {error}")
        
        if healthy_keys > 0:
            recommendations.append(f"✅ {healthy_keys}개 키가 정상 작동 중입니다.")
        
        return recommendations

    async def attempt_key_recovery(self, provider: APIProvider) -> Dict:
        """문제가 있는 키들의 복구 시도"""
        self.logger.info(f"🔄 {provider.value} API 키 복구 시도 시작")
        
        recovery_results = {
            "provider": provider.value,
            "started_at": datetime.now().isoformat(),
            "attempted_keys": 0,
            "recovered_keys": 0,
            "failed_keys": 0,
            "key_results": []
        }

        detailed_status = self.key_manager.get_detailed_key_status(provider)
        if "error" in detailed_status:
            recovery_results["error"] = detailed_status["error"]
            return recovery_results

        # 비활성화된 키들 중 복구 가능한 것들 찾기
        for key_info in detailed_status["keys"]:
            if not key_info["is_active"] and key_info["error_count"] >= 5:
                # 마지막 오류가 30분 이상 지났으면 복구 시도
                if key_info["last_error_time"]:
                    last_error = datetime.fromisoformat(key_info["last_error_time"])
                    if datetime.now() - last_error > timedelta(minutes=30):
                        await self._attempt_single_key_recovery(provider, key_info, recovery_results)
                        recovery_results["attempted_keys"] += 1

        recovery_results["completed_at"] = datetime.now().isoformat()
        
        self.logger.info(
            f"✅ {provider.value} API 키 복구 시도 완료: "
            f"{recovery_results['recovered_keys']}/{recovery_results['attempted_keys']}개 복구됨"
        )
        
        return recovery_results

    async def _attempt_single_key_recovery(self, provider: APIProvider, key_info: Dict, recovery_results: Dict):
        """개별 키 복구 시도"""
        key_preview = key_info["key_preview"]
        self.logger.info(f"🔧 {provider.value} 키 #{key_info['index']} 복구 시도: {key_preview}")
        
        key_result = {
            "index": key_info["index"],
            "key_preview": key_preview,
            "recovery_attempted": True,
            "recovery_successful": False,
            "error": None
        }

        try:
            # 키 재활성화
            success = self.key_manager.reactivate_key(provider, key_preview)
            
            if success:
                # 간단한 테스트 호출
                async with self.api_client:
                    test_result = await self._test_single_key(provider, {
                        **key_info,
                        "is_active": True,
                        "is_available": True,
                        "error_count": 0
                    })
                
                if test_result["is_healthy"]:
                    key_result["recovery_successful"] = True
                    recovery_results["recovered_keys"] += 1
                    self.logger.info(f"✅ {provider.value} 키 #{key_info['index']} 복구 성공: {key_preview}")
                else:
                    # 테스트 실패시 다시 비활성화
                    self.key_manager.force_deactivate_key(provider, key_preview, "복구 테스트 실패")
                    key_result["error"] = test_result["error"]
                    recovery_results["failed_keys"] += 1
                    self.logger.warning(f"❌ {provider.value} 키 #{key_info['index']} 복구 실패: {key_preview}")
            else:
                key_result["error"] = "재활성화 실패"
                recovery_results["failed_keys"] += 1

        except Exception as e:
            key_result["error"] = str(e)
            recovery_results["failed_keys"] += 1
            self.logger.error(f"🔥 {provider.value} 키 #{key_info['index']} 복구 예외: {key_preview} - {e}")

        recovery_results["key_results"].append(key_result)

    def get_quick_status_summary(self, provider: APIProvider) -> Dict:
        """빠른 상태 요약 조회"""
        try:
            detailed_status = self.key_manager.get_detailed_key_status(provider)
            if "error" in detailed_status:
                return detailed_status

            summary = {
                "provider": provider.value,
                "timestamp": datetime.now().isoformat(),
                "total_keys": detailed_status["total_keys"],
                "active_keys": detailed_status["active_keys"],
                "available_keys": detailed_status["available_keys"],
                "health_percentage": (
                    (detailed_status["available_keys"] / detailed_status["total_keys"] * 100)
                    if detailed_status["total_keys"] > 0 else 0
                ),
                "current_key_index": detailed_status["current_key_index"],
                "status": "healthy" if detailed_status["available_keys"] > 0 else "unhealthy"
            }

            # 문제 키 개수 및 유형 분석
            problem_keys = []
            for key_info in detailed_status["keys"]:
                if not key_info["is_available"]:
                    problem_keys.append({
                        "index": key_info["index"],
                        "key_preview": key_info["key_preview"],
                        "issue": key_info["unavailable_reason"]
                    })
            
            summary["problem_keys"] = problem_keys
            summary["problem_count"] = len(problem_keys)

            return summary

        except Exception as e:
            return {"error": f"상태 요약 조회 실패: {e}"}

    def print_status_report(self, provider: APIProvider):
        """사용자 친화적인 상태 보고서 출력"""
        summary = self.get_quick_status_summary(provider)
        
        if "error" in summary:
            self.logger.error(f"❌ 상태 보고서 생성 실패: {summary['error']}")
            return

        print(f"\n{'='*60}")
        print(f"🔑 {provider.value} API 키 상태 보고서")
        print(f"{'='*60}")
        print(f"📊 전체 키: {summary['total_keys']}개")
        print(f"🟢 활성 키: {summary['active_keys']}개")
        print(f"✅ 사용 가능 키: {summary['available_keys']}개")
        print(f"📈 건강도: {summary['health_percentage']:.1f}%")
        print(f"🎯 현재 선택된 키: #{summary['current_key_index']}")
        print(f"💊 전체 상태: {summary['status']}")
        
        if summary['problem_count'] > 0:
            print(f"\n⚠️ 문제가 있는 키 ({summary['problem_count']}개):")
            for problem in summary['problem_keys']:
                print(f"  - 키 #{problem['index']} ({problem['key_preview']}): {problem['issue']}")
        else:
            print("\n✨ 모든 키가 정상 상태입니다!")
        
        print(f"{'='*60}\n")


# 전역 인스턴스
_api_key_monitor = None


def get_api_key_monitor() -> APIKeyMonitor:
    """API 키 모니터 싱글톤 인스턴스 반환"""
    global _api_key_monitor
    if _api_key_monitor is None:
        _api_key_monitor = APIKeyMonitor()
    return _api_key_monitor


# CLI 도구로 사용할 수 있는 메인 함수
async def main():
    """명령줄에서 직접 실행할 수 있는 메인 함수"""
    import sys
    
    monitor = get_api_key_monitor()
    
    if len(sys.argv) > 1 and sys.argv[1] == "health":
        # 건강 상태 점검
        results = await monitor.check_all_keys_health(APIProvider.KTO)
        print("\n🔍 KTO API 키 건강 상태 점검 결과:")
        print(f"건강한 키: {results['healthy_keys']}/{results['total_keys']}개")
        
        if results['recommendations']:
            print("\n💡 권장사항:")
            for rec in results['recommendations']:
                print(f"  {rec}")
    
    elif len(sys.argv) > 1 and sys.argv[1] == "recover":
        # 키 복구 시도
        results = await monitor.attempt_key_recovery(APIProvider.KTO)
        print("\n🔄 KTO API 키 복구 시도 결과:")
        print(f"복구됨: {results['recovered_keys']}/{results['attempted_keys']}개")
    
    else:
        # 빠른 상태 요약
        monitor.print_status_report(APIProvider.KTO)


if __name__ == "__main__":
    asyncio.run(main())