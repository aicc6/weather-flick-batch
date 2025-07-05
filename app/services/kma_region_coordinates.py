"""
기상청 API 지역별 격자 좌표 및 WGS84 좌표 정보

기상청에서 제공하는 전국 시도별 격자 좌표와 실제 위경도 좌표를 매핑합니다.
Lambert Conformal Conic 투영법 기반의 격자 좌표계를 사용합니다.
"""

# 기상청 전국 시도별 격자 좌표 및 WGS84 좌표 정보
KMA_REGION_COORDINATES = {
    # 특별시/광역시
    "서울": {
        "area_code": "1",
        "nx": 60, "ny": 127,
        "latitude": 37.5665, "longitude": 126.9780,
        "station_code": "108",
        "region_name_full": "서울특별시",
        "region_center": "중구"
    },
    "부산": {
        "area_code": "6", 
        "nx": 98, "ny": 76,
        "latitude": 35.1796, "longitude": 129.0756,
        "station_code": "159",
        "region_name_full": "부산광역시",
        "region_center": "중구"
    },
    "대구": {
        "area_code": "4",
        "nx": 89, "ny": 90,
        "latitude": 35.8714, "longitude": 128.6014,
        "station_code": "143",
        "region_name_full": "대구광역시", 
        "region_center": "중구"
    },
    "인천": {
        "area_code": "2",
        "nx": 55, "ny": 124,
        "latitude": 37.4563, "longitude": 126.7052,
        "station_code": "112",
        "region_name_full": "인천광역시",
        "region_center": "중구"
    },
    "광주": {
        "area_code": "5",
        "nx": 58, "ny": 74,
        "latitude": 35.1595, "longitude": 126.8526,
        "station_code": "156",
        "region_name_full": "광주광역시",
        "region_center": "동구"
    },
    "대전": {
        "area_code": "3",
        "nx": 67, "ny": 100,
        "latitude": 36.3504, "longitude": 127.3845,
        "station_code": "133",
        "region_name_full": "대전광역시",
        "region_center": "중구"
    },
    "울산": {
        "area_code": "7",
        "nx": 102, "ny": 84,
        "latitude": 35.5384, "longitude": 129.3114,
        "station_code": "152",
        "region_name_full": "울산광역시",
        "region_center": "중구"
    },
    "세종": {
        "area_code": "8",
        "nx": 66, "ny": 103,
        "latitude": 36.4800, "longitude": 127.2890,
        "station_code": "239",  # 세종 관측소
        "region_name_full": "세종특별자치시",
        "region_center": "세종시"
    },
    
    # 도 단위
    "경기": {
        "area_code": "31",
        "nx": 60, "ny": 120,  # 수원 기준
        "latitude": 37.2636, "longitude": 127.0286,  # 수원시청
        "station_code": "119",  # 수원 관측소
        "region_name_full": "경기도",
        "region_center": "수원시"
    },
    "강원": {
        "area_code": "32", 
        "nx": 73, "ny": 134,  # 춘천 기준
        "latitude": 37.8813, "longitude": 127.7298,  # 춘천시청
        "station_code": "101",  # 춘천 관측소
        "region_name_full": "강원특별자치도",
        "region_center": "춘천시"
    },
    "충북": {
        "area_code": "33",
        "nx": 69, "ny": 106,  # 청주 기준
        "latitude": 36.4800, "longitude": 127.2890,  # 청주시청
        "station_code": "131",  # 청주 관측소
        "region_name_full": "충청북도",
        "region_center": "청주시"
    },
    "충남": {
        "area_code": "34",
        "nx": 68, "ny": 100,  # 홍성 기준 (도청 소재지)
        "latitude": 36.6018, "longitude": 126.6750,  # 홍성군청
        "station_code": "177",  # 홍성 관측소
        "region_name_full": "충청남도",
        "region_center": "홍성군"
    },
    "경북": {
        "area_code": "35",
        "nx": 87, "ny": 106,  # 안동 기준 (도청 소재지)
        "latitude": 36.5684, "longitude": 128.7294,  # 안동시청
        "station_code": "136",  # 안동 관측소
        "region_name_full": "경상북도",
        "region_center": "안동시"
    },
    "경남": {
        "area_code": "36",
        "nx": 91, "ny": 77,  # 창원 기준 (도청 소재지)
        "latitude": 35.2280, "longitude": 128.6811,  # 창원시청
        "station_code": "155",  # 창원 관측소
        "region_name_full": "경상남도",
        "region_center": "창원시"
    },
    "전북": {
        "area_code": "37",
        "nx": 63, "ny": 89,  # 전주 기준 (도청 소재지)
        "latitude": 35.8242, "longitude": 127.1480,  # 전주시청
        "station_code": "146",  # 전주 관측소
        "region_name_full": "전북특별자치도",
        "region_center": "전주시"
    },
    "전남": {
        "area_code": "38",
        "nx": 51, "ny": 67,  # 무안 기준 (도청 소재지)
        "latitude": 34.8679, "longitude": 126.4630,  # 무안군청
        "station_code": "165",  # 목포 관측소 (인근)
        "region_name_full": "전라남도",
        "region_center": "무안군"
    },
    "제주": {
        "area_code": "39",
        "nx": 52, "ny": 38,
        "latitude": 33.4996, "longitude": 126.5312,
        "station_code": "184",  # 제주 관측소
        "region_name_full": "제주특별자치도",
        "region_center": "제주시"
    }
}

# 추가 기상청 관측소 정보 (주요 도시별)
ADDITIONAL_KMA_STATIONS = {
    # 경기도 주요 도시
    "고양": {"nx": 57, "ny": 128, "latitude": 37.6583, "longitude": 126.8320, "station_code": None},
    "성남": {"nx": 63, "ny": 124, "latitude": 37.4449, "longitude": 127.1388, "station_code": None},
    "용인": {"nx": 64, "ny": 119, "latitude": 37.2411, "longitude": 127.1776, "station_code": None},
    "부천": {"nx": 58, "ny": 125, "latitude": 37.5034, "longitude": 126.7660, "station_code": None},
    "안산": {"nx": 58, "ny": 121, "latitude": 37.3236, "longitude": 126.8219, "station_code": None},
    "안양": {"nx": 59, "ny": 123, "latitude": 37.3943, "longitude": 126.9568, "station_code": None},
    "남양주": {"nx": 64, "ny": 128, "latitude": 37.6361, "longitude": 127.2168, "station_code": None},
    
    # 강원도 주요 도시
    "강릉": {"nx": 92, "ny": 131, "latitude": 37.7519, "longitude": 128.8761, "station_code": "105"},
    "원주": {"nx": 76, "ny": 122, "latitude": 37.3422, "longitude": 127.9202, "station_code": "114"},
    
    # 충청북도 주요 도시
    "충주": {"nx": 76, "ny": 114, "latitude": 36.9910, "longitude": 127.9259, "station_code": "127"},
    
    # 충청남도 주요 도시
    "천안": {"nx": 63, "ny": 110, "latitude": 36.8151, "longitude": 127.1139, "station_code": "232"},
    "아산": {"nx": 60, "ny": 110, "latitude": 36.7898, "longitude": 127.0042, "station_code": None},
    
    # 경상북도 주요 도시
    "포항": {"nx": 102, "ny": 94, "latitude": 36.0190, "longitude": 129.3435, "station_code": "138"},
    "경주": {"nx": 100, "ny": 91, "latitude": 35.8562, "longitude": 129.2247, "station_code": None},
    "구미": {"nx": 84, "ny": 96, "latitude": 36.1196, "longitude": 128.3441, "station_code": "279"},
    
    # 경상남도 주요 도시
    "김해": {"nx": 95, "ny": 77, "latitude": 35.2341, "longitude": 128.8890, "station_code": None},
    "양산": {"nx": 97, "ny": 79, "latitude": 35.3350, "longitude": 129.0378, "station_code": None},
    "진주": {"nx": 90, "ny": 75, "latitude": 35.1800, "longitude": 128.1076, "station_code": "192"},
    
    # 전라북도 주요 도시
    "익산": {"nx": 60, "ny": 91, "latitude": 35.9483, "longitude": 126.9575, "station_code": None},
    "군산": {"nx": 56, "ny": 92, "latitude": 35.9675, "longitude": 126.7370, "station_code": "140"},
    
    # 전라남도 주요 도시
    "여수": {"nx": 73, "ny": 66, "latitude": 34.7604, "longitude": 127.6622, "station_code": "168"},
    "순천": {"nx": 70, "ny": 70, "latitude": 34.9506, "longitude": 127.4872, "station_code": None},
    "목포": {"nx": 50, "ny": 67, "latitude": 34.8118, "longitude": 126.3922, "station_code": "165"},
    
    # 제주도 주요 도시
    "서귀포": {"nx": 52, "ny": 33, "latitude": 33.2541, "longitude": 126.5600, "station_code": "189"}
}

def get_all_kma_regions():
    """모든 기상청 지역 정보 반환"""
    return KMA_REGION_COORDINATES

def get_kma_region_by_name(region_name: str):
    """지역명으로 기상청 지역 정보 조회"""
    return KMA_REGION_COORDINATES.get(region_name)

def get_kma_region_by_area_code(area_code: str):
    """지역 코드로 기상청 지역 정보 조회"""
    for region_name, data in KMA_REGION_COORDINATES.items():
        if data["area_code"] == area_code:
            return {region_name: data}
    return None

def get_additional_stations():
    """추가 기상청 관측소 정보 반환"""
    return ADDITIONAL_KMA_STATIONS

def convert_kma_grid_to_wgs84(nx: int, ny: int) -> tuple:
    """기상청 격자 좌표를 WGS84로 변환 (근사치)"""
    # 기존 매핑 테이블에서 가장 가까운 좌표 찾기
    min_distance = float('inf')
    closest_coords = (0.0, 0.0)
    
    for region_data in KMA_REGION_COORDINATES.values():
        grid_distance = ((nx - region_data["nx"]) ** 2 + (ny - region_data["ny"]) ** 2) ** 0.5
        if grid_distance < min_distance:
            min_distance = grid_distance
            closest_coords = (region_data["latitude"], region_data["longitude"])
    
    return closest_coords

def get_region_by_coordinates(lat: float, lon: float, threshold: float = 0.5):
    """좌표로 가장 가까운 지역 찾기"""
    min_distance = float('inf')
    closest_region = None
    
    # 메인 지역 검색
    for region_name, data in KMA_REGION_COORDINATES.items():
        distance = ((lat - data["latitude"]) ** 2 + (lon - data["longitude"]) ** 2) ** 0.5
        if distance < min_distance:
            min_distance = distance
            closest_region = region_name
    
    # 추가 관측소 검색
    for station_name, data in ADDITIONAL_KMA_STATIONS.items():
        distance = ((lat - data["latitude"]) ** 2 + (lon - data["longitude"]) ** 2) ** 0.5
        if distance < min_distance:
            min_distance = distance
            closest_region = station_name
    
    # 임계값 내에 있는 경우만 반환
    if min_distance <= threshold:
        return closest_region, min_distance
    
    return None, min_distance