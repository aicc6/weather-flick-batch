"""
한국관광공사 수집 데이터 분석 모듈

수집된 관광 데이터를 분석하고 정리하는 기능을 제공합니다.
"""

import json
import pandas as pd
from typing import Dict, List
from datetime import datetime


class KTODataAnalyzer:
    """한국관광공사 데이터 분석기"""

    def __init__(self):
        self.area_codes = self.load_json_data("area_codes.json")
        self.tourist_attractions = self.load_json_data("tourist_attractions.json")
        self.festivals_events = self.load_json_data("festivals_events.json")

    def load_json_data(self, filename: str) -> List[Dict]:
        """JSON 파일에서 데이터 로드"""
        try:
            with open(filename, "r", encoding="utf-8") as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"파일을 찾을 수 없습니다: {filename}")
            return []
        except json.JSONDecodeError:
            print(f"JSON 파싱 오류: {filename}")
            return []

    def analyze_area_codes(self) -> Dict:
        """지역 코드 데이터 분석"""
        if not self.area_codes:
            return {"error": "지역 코드 데이터가 없습니다."}

        analysis = {
            "총_지역_수": len(self.area_codes),
            "광역시도_목록": [],
            "지역별_코드_매핑": {},
        }

        for area in self.area_codes:
            area_name = area.get("name", "N/A")
            area_code = area.get("code", "N/A")
            analysis["광역시도_목록"].append(area_name)
            analysis["지역별_코드_매핑"][area_name] = area_code

        return analysis

    def analyze_tourist_attractions(self) -> Dict:
        """관광지 데이터 분석"""
        if not self.tourist_attractions:
            return {"error": "관광지 데이터가 없습니다."}

        # 지역별 관광지 분포
        area_distribution = {}
        content_type_distribution = {}
        attractions_with_coordinates = 0
        attractions_with_images = 0

        for attraction in self.tourist_attractions:
            # 지역별 분포
            area_code = attraction.get("areacode", "Unknown")
            area_name = self.get_area_name_by_code(area_code)
            if area_name not in area_distribution:
                area_distribution[area_name] = 0
            area_distribution[area_name] += 1

            # 콘텐츠 유형별 분포
            content_type = attraction.get("contenttypeid", "Unknown")
            content_type_name = self.get_content_type_name(content_type)
            if content_type_name not in content_type_distribution:
                content_type_distribution[content_type_name] = 0
            content_type_distribution[content_type_name] += 1

            # 좌표 정보 유무
            if attraction.get("mapx") and attraction.get("mapy"):
                attractions_with_coordinates += 1

            # 이미지 정보 유무
            if attraction.get("firstimage"):
                attractions_with_images += 1

        analysis = {
            "총_관광지_수": len(self.tourist_attractions),
            "지역별_분포": area_distribution,
            "콘텐츠_유형별_분포": content_type_distribution,
            "좌표_정보_있는_관광지": attractions_with_coordinates,
            "이미지_있는_관광지": attractions_with_images,
            "데이터_완성도": {
                "좌표_정보_완성도": f"{(attractions_with_coordinates / len(self.tourist_attractions) * 100):.1f}%",
                "이미지_완성도": f"{(attractions_with_images / len(self.tourist_attractions) * 100):.1f}%",
            },
            "주요_관광지_목록": [
                {
                    "이름": attr.get("title", "N/A"),
                    "주소": attr.get("addr1", "N/A"),
                    "지역": self.get_area_name_by_code(attr.get("areacode", "")),
                    "전화번호": attr.get("tel", "N/A"),
                }
                for attr in self.tourist_attractions
            ],
        }

        return analysis

    def analyze_festivals_events(self) -> Dict:
        """축제/행사 데이터 분석"""
        if not self.festivals_events:
            return {"error": "축제/행사 데이터가 없습니다."}

        # 지역별 축제 분포
        area_distribution = {}
        monthly_distribution = {}
        current_events = []
        upcoming_events = []

        current_date = datetime.now().strftime("%Y%m%d")

        for event in self.festivals_events:
            # 지역별 분포
            area_code = event.get("areacode", "Unknown")
            area_name = self.get_area_name_by_code(area_code)
            if area_name not in area_distribution:
                area_distribution[area_name] = 0
            area_distribution[area_name] += 1

            # 월별 분포 (시작일 기준)
            start_date = event.get("eventstartdate", "")
            if len(start_date) >= 6:
                month = start_date[4:6]
                month_name = f"{int(month)}월"
                if month_name not in monthly_distribution:
                    monthly_distribution[month_name] = 0
                monthly_distribution[month_name] += 1

            # 현재/예정 이벤트 분류
            start_date = event.get("eventstartdate", "")
            end_date = event.get("eventenddate", "")

            if start_date <= current_date <= end_date:
                current_events.append(event)
            elif start_date > current_date:
                upcoming_events.append(event)

        analysis = {
            "총_축제행사_수": len(self.festivals_events),
            "지역별_분포": area_distribution,
            "월별_분포": monthly_distribution,
            "현재_진행중인_행사": len(current_events),
            "예정된_행사": len(upcoming_events),
            "행사_목록": [
                {
                    "이름": event.get("title", "N/A"),
                    "지역": self.get_area_name_by_code(event.get("areacode", "")),
                    "시작일": event.get("eventstartdate", "N/A"),
                    "종료일": event.get("eventenddate", "N/A"),
                    "주소": event.get("addr1", "N/A"),
                    "상태": "진행중"
                    if event in current_events
                    else "예정"
                    if event in upcoming_events
                    else "종료",
                }
                for event in self.festivals_events
            ],
        }

        return analysis

    def get_area_name_by_code(self, area_code: str) -> str:
        """지역 코드로 지역명 조회"""
        for area in self.area_codes:
            if area.get("code") == str(area_code):
                return area.get("name", f"코드_{area_code}")
        return f"코드_{area_code}"

    def get_content_type_name(self, content_type_id: str) -> str:
        """콘텐츠 타입 ID로 이름 반환"""
        content_types = {
            "12": "관광지",
            "14": "문화시설",
            "15": "축제공연행사",
            "25": "여행코스",
            "28": "레포츠",
            "32": "숙박",
            "38": "쇼핑",
            "39": "음식점",
        }
        return content_types.get(str(content_type_id), f"타입_{content_type_id}")

    def generate_summary_report(self) -> Dict:
        """전체 데이터 요약 보고서 생성"""
        area_analysis = self.analyze_area_codes()
        attractions_analysis = self.analyze_tourist_attractions()
        festivals_analysis = self.analyze_festivals_events()

        report = {
            "데이터_수집_일시": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "데이터_소스": "한국관광공사 API (샘플 데이터)",
            "수집_현황": {
                "지역_코드": area_analysis.get("총_지역_수", 0),
                "관광지_정보": attractions_analysis.get("총_관광지_수", 0),
                "축제행사_정보": festivals_analysis.get("총_축제행사_수", 0),
            },
            "주요_지역": list(area_analysis.get("지역별_코드_매핑", {}).keys())[:5],
            "데이터_품질": {
                "관광지_좌표_완성도": attractions_analysis.get("데이터_완성도", {}).get(
                    "좌표_정보_완성도", "N/A"
                ),
                "관광지_이미지_완성도": attractions_analysis.get(
                    "데이터_완성도", {}
                ).get("이미지_완성도", "N/A"),
            },
            "추천_분석": self.generate_recommendations(),
        }

        return report

    def generate_recommendations(self) -> Dict:
        """데이터 기반 추천 분석"""
        recommendations = {
            "인기_지역_TOP3": [],
            "여름_추천_관광지": [],
            "계절별_축제": {"봄": [], "여름": [], "가을": [], "겨울": []},
        }

        # 관광지 수 기준 인기 지역
        if self.tourist_attractions:
            area_count = {}
            for attraction in self.tourist_attractions:
                area_name = self.get_area_name_by_code(attraction.get("areacode", ""))
                area_count[area_name] = area_count.get(area_name, 0) + 1

            sorted_areas = sorted(area_count.items(), key=lambda x: x[1], reverse=True)
            recommendations["인기_지역_TOP3"] = [area[0] for area in sorted_areas[:3]]

        # 여름 추천 관광지 (해수욕장 포함)
        summer_keywords = ["해수욕장", "해변", "바다", "워터파크"]
        for attraction in self.tourist_attractions:
            title = attraction.get("title", "").lower()
            if any(keyword in title for keyword in summer_keywords):
                recommendations["여름_추천_관광지"].append(attraction.get("title", ""))

        # 계절별 축제 분류
        for event in self.festivals_events:
            start_date = event.get("eventstartdate", "")
            if len(start_date) >= 6:
                month = int(start_date[4:6])
                event_title = event.get("title", "")

                if 3 <= month <= 5:
                    recommendations["계절별_축제"]["봄"].append(event_title)
                elif 6 <= month <= 8:
                    recommendations["계절별_축제"]["여름"].append(event_title)
                elif 9 <= month <= 11:
                    recommendations["계절별_축제"]["가을"].append(event_title)
                else:
                    recommendations["계절별_축제"]["겨울"].append(event_title)

        return recommendations

    def export_to_excel(self, filename: str = "tourism_data_analysis.xlsx"):
        """분석 결과를 Excel 파일로 저장"""
        try:
            with pd.ExcelWriter(filename, engine="openpyxl") as writer:
                # 지역 코드 시트
                if self.area_codes:
                    area_df = pd.DataFrame(self.area_codes)
                    area_df.to_excel(writer, sheet_name="지역코드", index=False)

                # 관광지 시트
                if self.tourist_attractions:
                    attractions_df = pd.DataFrame(self.tourist_attractions)
                    attractions_df.to_excel(
                        writer, sheet_name="관광지정보", index=False
                    )

                # 축제행사 시트
                if self.festivals_events:
                    festivals_df = pd.DataFrame(self.festivals_events)
                    festivals_df.to_excel(writer, sheet_name="축제행사", index=False)

                # 요약 분석 시트
                summary = self.generate_summary_report()
                summary_df = pd.DataFrame([summary])
                summary_df.to_excel(writer, sheet_name="요약분석", index=False)

            print(f"분석 결과가 {filename}에 저장되었습니다.")

        except Exception as e:
            print(f"Excel 파일 저장 오류: {e}")


def main():
    """메인 실행 함수"""
    print("=== 한국관광공사 데이터 분석 시작 ===")

    analyzer = KTODataAnalyzer()

    # 각 데이터 유형별 분석
    print("\n1. 지역 코드 분석")
    area_analysis = analyzer.analyze_area_codes()
    print(json.dumps(area_analysis, ensure_ascii=False, indent=2))

    print("\n2. 관광지 데이터 분석")
    attractions_analysis = analyzer.analyze_tourist_attractions()
    print(json.dumps(attractions_analysis, ensure_ascii=False, indent=2))

    print("\n3. 축제/행사 데이터 분석")
    festivals_analysis = analyzer.analyze_festivals_events()
    print(json.dumps(festivals_analysis, ensure_ascii=False, indent=2))

    print("\n4. 전체 요약 보고서")
    summary_report = analyzer.generate_summary_report()
    print(json.dumps(summary_report, ensure_ascii=False, indent=2))

    # Excel 파일로 저장
    print("\n5. Excel 파일 저장")
    analyzer.export_to_excel()

    print("\n=== 데이터 분석 완료 ===")


if __name__ == "__main__":
    main()

