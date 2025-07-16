#!/usr/bin/env python3
"""
cultural_facilities 테이블에 대한 upsert 함수 추가 스크립트
"""

# upsert_cultural_facility 함수 내용
function_code = '''
    def upsert_cultural_facility(self, data: Dict) -> bool:
        """문화시설 데이터 UPSERT"""

        query = """
        INSERT INTO cultural_facilities (
            content_id, region_code, facility_name, category_code, category_name,
            address, detail_address, zipcode, latitude, longitude,
            tel, homepage, overview, first_image, first_image_small,
            booktour, createdtime, modifiedtime, telname, faxno, mlevel,
            detail_intro_info, detail_additional_info,
            raw_data_id, last_sync_at, data_quality_score
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (content_id) DO UPDATE SET
            region_code = EXCLUDED.region_code,
            facility_name = EXCLUDED.facility_name,
            category_code = EXCLUDED.category_code,
            category_name = EXCLUDED.category_name,
            address = EXCLUDED.address,
            detail_address = EXCLUDED.detail_address,
            zipcode = EXCLUDED.zipcode,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            tel = EXCLUDED.tel,
            homepage = EXCLUDED.homepage,
            overview = EXCLUDED.overview,
            first_image = EXCLUDED.first_image,
            first_image_small = EXCLUDED.first_image_small,
            booktour = EXCLUDED.booktour,
            createdtime = EXCLUDED.createdtime,
            modifiedtime = EXCLUDED.modifiedtime,
            telname = EXCLUDED.telname,
            faxno = EXCLUDED.faxno,
            mlevel = EXCLUDED.mlevel,
            detail_intro_info = EXCLUDED.detail_intro_info,
            detail_additional_info = EXCLUDED.detail_additional_info,
            raw_data_id = EXCLUDED.raw_data_id,
            last_sync_at = EXCLUDED.last_sync_at,
            data_quality_score = EXCLUDED.data_quality_score,
            updated_at = CURRENT_TIMESTAMP
        """

        params = (
            data.get("content_id"),
            data.get("region_code"),
            data.get("facility_name") or data.get("title"),
            data.get("category_code") or data.get("cat1"),
            data.get("category_name") or data.get("cat3"),
            data.get("address") or data.get("addr1"),
            data.get("detail_address") or data.get("addr2"),
            data.get("zipcode") or data.get("zip_code"),
            data.get("latitude"),
            data.get("longitude"),
            data.get("tel"),
            data.get("homepage"),
            data.get("overview") or data.get("description"),
            data.get("first_image") or data.get("firstimage"),
            data.get("first_image_small") or data.get("firstimage2"),
            # 새로 추가된 필드들
            data.get("booktour") or data.get("book_tour"),
            data.get("createdtime") or data.get("created_time"),
            data.get("modifiedtime") or data.get("modified_time"),
            data.get("telname") or data.get("tel_name"),
            data.get("faxno") or data.get("fax_no"),
            data.get("mlevel") or data.get("map_level"),
            json.dumps(data.get("detail_intro_info") or data.get("intro_info"), ensure_ascii=False) if data.get("detail_intro_info") or data.get("intro_info") else None,
            json.dumps(data.get("detail_additional_info") or data.get("additional_info"), ensure_ascii=False) if data.get("detail_additional_info") or data.get("additional_info") else None,
            # 메타데이터 필드들
            data.get("raw_data_id"),
            data.get("last_sync_at"),
            data.get("data_quality_score"),
        )

        try:
            self.db_manager.execute_update(query, params)
            return True
        except Exception as e:
            self.logger.error(f"문화시설 데이터 UPSERT 실패: {e}")
            return False
'''

# 파일 경로
file_path = "/Users/sl/Repository/aicc6/weather-flick-batch/app/core/database_manager_extension.py"

# 파일 읽기
with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

# upsert_festival_event 함수 찾기
import re

# upsert_festival_event 함수 끝 부분 찾기
pattern = r'(def upsert_festival_event.*?\n\s+return False)'
match = re.search(pattern, content, re.DOTALL)

if match:
    # upsert_festival_event 함수 끝 위치 찾기
    end_pos = match.end()
    
    # 새로운 함수 추가
    new_content = content[:end_pos] + "\n" + function_code + content[end_pos:]
    
    # 파일에 쓰기
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(new_content)
    
    print("upsert_cultural_facility 함수가 추가되었습니다.")
else:
    print("upsert_festival_event 함수를 찾을 수 없습니다.")