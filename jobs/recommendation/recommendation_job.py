"""
추천 엔진 배치 작업

과거 날씨 데이터를 기반으로 지역별 여행 추천 점수를 계산하고 저장하는 배치 작업입니다.
"""

from datetime import datetime
from typing import Dict, List

from app.core.base_job import BaseJob, JobResult, JobConfig
from jobs.recommendation.travel_recommendation_engine import (
    WeatherBasedRecommendationEngine,
)
from app.core.database_manager import DatabaseManager


class RecommendationJob(BaseJob):
    """추천 점수 계산 배치 작업"""

    def __init__(self, config: JobConfig):
        super().__init__(config)
        self.recommendation_engine = WeatherBasedRecommendationEngine()
        self.db_manager = DatabaseManager()
        self.processed_records = 0

    def execute(self) -> JobResult:
        """추천 점수 계산 실행"""
        result = JobResult(
            job_name=self.config.job_name,
            job_type=self.config.job_type,
            status="running",
            start_time=datetime.now(),
        )

        try:
            # 활성 지역 조회
            active_regions = self._get_active_regions()
            self.logger.info(f"추천 계산 대상 지역: {len(active_regions)}개")

            # 지역별 추천 점수 계산
            total_recommendations = 0
            for region in active_regions:
                try:
                    recommendations = self._calculate_region_recommendations(region)
                    saved_count = self._save_recommendations(recommendations)
                    total_recommendations += saved_count
                    self.logger.debug(
                        f"지역 {region['region_name']} 추천 점수 {saved_count}건 저장"
                    )
                except Exception as e:
                    self.logger.warning(
                        f"지역 {region['region_name']} 추천 계산 실패: {str(e)}"
                    )

            result.processed_records = total_recommendations
            result.metadata = {
                "regions_processed": len(active_regions),
                "recommendations_generated": total_recommendations,
            }

            self.logger.info(f"추천 점수 계산 완료: 총 {total_recommendations}건 처리")

        except Exception as e:
            self.logger.error(f"추천 점수 계산 실패: {str(e)}")
            raise

        return result

    def _get_active_regions(self) -> List[Dict]:
        """활성 지역 목록 조회"""
        query = """
        SELECT DISTINCT
            region_code,
            COALESCE(
                (SELECT region_name FROM regions WHERE region_code = hwd.region_code LIMIT 1),
                CASE
                    WHEN region_code = '11' THEN '서울'
                    WHEN region_code = '26' THEN '부산'
                    WHEN region_code = '27' THEN '대구'
                    WHEN region_code = '28' THEN '인천'
                    WHEN region_code = '29' THEN '광주'
                    WHEN region_code = '30' THEN '대전'
                    WHEN region_code = '31' THEN '울산'
                    WHEN region_code = '36' THEN '세종'
                    WHEN region_code = '41' THEN '경기'
                    WHEN region_code = '42' THEN '강원'
                    WHEN region_code = '43' THEN '충북'
                    WHEN region_code = '44' THEN '충남'
                    WHEN region_code = '45' THEN '전북'
                    WHEN region_code = '46' THEN '전남'
                    WHEN region_code = '47' THEN '경북'
                    WHEN region_code = '48' THEN '경남'
                    WHEN region_code = '50' THEN '제주'
                    ELSE '기타'
                END
            ) as region_name
        FROM historical_weather_daily hwd
        WHERE weather_date >= CURRENT_DATE - INTERVAL '1 year'
        AND region_code != '00'
        ORDER BY region_code
        """

        return self.db_manager.execute_query(query)

    def _calculate_region_recommendations(self, region: Dict) -> List[Dict]:
        """지역별 추천 점수 계산"""
        # 최근 1년간의 과거 날씨 데이터 조회
        weather_data = self._get_historical_weather_data(region["region_code"])

        if not weather_data:
            self.logger.warning(f"지역 {region['region_name']} 날씨 데이터 없음")
            return []

        # 추천 엔진을 통한 월별 추천 계산
        region_info = {
            "region_code": region["region_code"],
            "region_name": region["region_name"],
            "attraction_id": None,
        }

        recommendations = self.recommendation_engine.calculate_monthly_recommendations(
            weather_data, region_info
        )

        # 데이터베이스 저장 형식으로 변환
        db_recommendations = []
        for rec in recommendations:
            db_rec = {
                "region_code": rec.region_code,
                "attraction_id": rec.attraction_id,
                "date_period": rec.date_period.replace(year=datetime.now().year).date(),
                "weather_score": rec.weather_score.overall_score,
                "temperature_score": rec.weather_score.temperature_score,
                "precipitation_score": rec.weather_score.precipitation_score,
                "humidity_score": rec.weather_score.humidity_score,
                "overall_score": rec.weather_score.overall_score,
                "recommendation_level": rec.recommendation_level.value,
            }
            db_recommendations.append(db_rec)

        return db_recommendations

    def _get_historical_weather_data(self, region_code: str) -> List[Dict]:
        """특정 지역의 과거 날씨 데이터 조회"""
        query = """
        SELECT
            weather_date,
            avg_temp,
            max_temp,
            min_temp,
            humidity,
            precipitation,
            wind_speed,
            weather_condition
        FROM historical_weather_daily
        WHERE region_code = %s
        AND weather_date >= CURRENT_DATE - INTERVAL '2 year'
        AND avg_temp IS NOT NULL
        ORDER BY weather_date
        """

        return self.db_manager.execute_query(query, (region_code,))

    def _save_recommendations(self, recommendations: List[Dict]) -> int:
        """추천 점수를 데이터베이스에 저장"""
        if not recommendations:
            return 0

        saved_count = 0
        for recommendation in recommendations:
            try:
                query = """
                INSERT INTO travel_weather_scores
                (region_code, attraction_id, date_period, weather_score, temperature_score,
                 precipitation_score, humidity_score, overall_score, recommendation_level)
                VALUES (%(region_code)s, %(attraction_id)s, %(date_period)s, %(weather_score)s,
                        %(temperature_score)s, %(precipitation_score)s, %(humidity_score)s,
                        %(overall_score)s, %(recommendation_level)s)
                ON CONFLICT (region_code, date_period) DO UPDATE SET
                weather_score = EXCLUDED.weather_score,
                temperature_score = EXCLUDED.temperature_score,
                precipitation_score = EXCLUDED.precipitation_score,
                humidity_score = EXCLUDED.humidity_score,
                overall_score = EXCLUDED.overall_score,
                recommendation_level = EXCLUDED.recommendation_level,
                updated_at = CURRENT_TIMESTAMP
                """

                # recommendation_level이 enum이 아닌 문자열로 전달되도록 변환
                if isinstance(recommendation["recommendation_level"], str):
                    recommendation["recommendation_level"] = recommendation[
                        "recommendation_level"
                    ]
                else:
                    recommendation["recommendation_level"] = str(
                        recommendation["recommendation_level"]
                    )

                affected_rows = self.db_manager.execute_update(query, recommendation)
                if affected_rows > 0:
                    saved_count += 1

            except Exception as e:
                self.logger.warning(
                    f"추천 점수 저장 실패 [{recommendation.get('region_code', 'unknown')}]: {e}"
                )
                continue

        self.logger.debug(f"추천 점수 {saved_count}건 저장")
        return saved_count

    def pre_execute(self) -> bool:
        """실행 전 검증"""
        # 데이터베이스 연결 확인
        try:
            self.db_manager.execute_query("SELECT 1")
            return True
        except Exception as e:
            self.logger.error(f"데이터베이스 연결 실패: {e}")
            return False

    def post_execute(self, result: JobResult) -> None:
        """실행 후 처리"""
        super().post_execute(result)

        # 성능 메트릭 로그
        if result.duration_seconds > 0:
            records_per_second = result.processed_records / result.duration_seconds
            self.logger.info(f"처리 성능: {records_per_second:.2f} records/second")

        # 추천 점수 통계 로그
        try:
            stats_query = """
            SELECT
                recommendation_level,
                COUNT(*) as count
            FROM travel_weather_scores
            WHERE updated_at >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
            GROUP BY recommendation_level
            ORDER BY recommendation_level
            """
            stats = self.db_manager.execute_query(stats_query)
            self.logger.info(
                f"추천 등급별 통계: {dict((s['recommendation_level'], s['count']) for s in stats)}"
            )
        except Exception as e:
            self.logger.warning(f"통계 조회 실패: {e}")
