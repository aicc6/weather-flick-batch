#!/usr/bin/env python3
"""
데이터베이스 직접 연결을 통한 원본 데이터 분석
"""

import os
import psycopg2
import psycopg2.extras
from datetime import datetime

# .env 파일 로드
from dotenv import load_dotenv
load_dotenv(override=True)


def get_db_connection():
    """데이터베이스 연결 생성"""
    return psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=int(os.getenv('DB_PORT', 5432)),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )


def analyze_api_raw_data():
    """api_raw_data 테이블 분석"""
    print("=== API 원본 데이터 테이블 분석 ===")
    
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # 1. 전체 레코드 수
            cur.execute("SELECT COUNT(*) as total_count FROM api_raw_data")
            result = cur.fetchone()
            total_count = result['total_count']
            print(f"총 원본 데이터 레코드 수: {total_count:,}개")
            
            if total_count == 0:
                print("⚠️ api_raw_data 테이블에 데이터가 없습니다.")
                return
            
            # 2. API 제공자별 분포
            cur.execute("""
                SELECT 
                    api_provider,
                    COUNT(*) as count,
                    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM api_raw_data), 2) as percentage
                FROM api_raw_data 
                GROUP BY api_provider 
                ORDER BY count DESC
            """)
            
            providers = cur.fetchall()
            print(f"\n📊 API 제공자별 데이터 분포:")
            for row in providers:
                print(f"  - {row['api_provider']}: {row['count']:,}개 ({row['percentage']}%)")
            
            # 3. 엔드포인트별 분포 (KTO 데이터만)
            cur.execute("""
                SELECT 
                    endpoint,
                    COUNT(*) as count,
                    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM api_raw_data WHERE api_provider = 'KTO'), 2) as percentage
                FROM api_raw_data 
                WHERE api_provider = 'KTO'
                GROUP BY endpoint 
                ORDER BY count DESC
            """)
            
            endpoints = cur.fetchall()
            print(f"\n🔗 KTO API 엔드포인트별 데이터 분포:")
            for row in endpoints:
                print(f"  - {row['endpoint']}: {row['count']:,}개 ({row['percentage']}%)")
            
            # 4. 날짜별 수집 현황
            cur.execute("""
                SELECT 
                    DATE(created_at) as collection_date,
                    COUNT(*) as count
                FROM api_raw_data 
                GROUP BY DATE(created_at) 
                ORDER BY collection_date DESC
                LIMIT 10
            """)
            
            dates = cur.fetchall()
            print(f"\n📅 최근 10일간 수집 현황:")
            for row in dates:
                print(f"  - {row['collection_date']}: {row['count']:,}개")
            
            # 5. 성공/실패 현황
            cur.execute("""
                SELECT 
                    success,
                    COUNT(*) as count,
                    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM api_raw_data), 2) as percentage
                FROM api_raw_data 
                GROUP BY success 
                ORDER BY count DESC
            """)
            
            statuses = cur.fetchall()
            print(f"\n✅ API 호출 성공/실패 현황:")
            for row in statuses:
                status_text = "성공" if row['success'] else "실패"
                print(f"  - {status_text}: {row['count']:,}개 ({row['percentage']}%)")
            
            # 6. 최신 수집 데이터 샘플
            cur.execute("""
                SELECT 
                    api_provider,
                    endpoint,
                    params::text,
                    success,
                    created_at
                FROM api_raw_data 
                ORDER BY created_at DESC
                LIMIT 5
            """)
            
            latest = cur.fetchall()
            print(f"\n🕒 최신 수집 데이터 샘플 (상위 5개):")
            for row in latest:
                params_preview = str(row['params'])[:50] + "..." if len(str(row['params'])) > 50 else str(row['params'])
                status_icon = "✅" if row['success'] else "❌"
                print(f"  {status_icon} {row['api_provider']}/{row['endpoint']} - {row['created_at']} - {params_preview}")
            
    except Exception as e:
        print(f"❌ 데이터베이스 분석 중 오류 발생: {e}")
    finally:
        conn.close()


def analyze_new_api_tables():
    """새로 추가된 4개 API 테이블 분석"""
    print("\n\n=== 신규 API 테이블 분석 ===")
    
    new_tables = [
        ('pet_tour_info', 'detailPetTour2'),
        ('classification_system_codes', 'lclsSystmCode2'),
        ('area_based_sync_list', 'areaBasedSyncList2'),
        ('legal_dong_codes', 'ldongCode2')
    ]
    
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            for table_name, api_name in new_tables:
                print(f"\n📋 {table_name} ({api_name}) 테이블:")
                
                # 테이블 존재 확인
                cur.execute(f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = '{table_name}'
                    )
                """)
                
                result = cur.fetchone()
                table_exists = result['exists']
                
                if not table_exists:
                    print(f"  ⚠️ 테이블이 존재하지 않습니다.")
                    continue
                
                # 레코드 수 조회
                cur.execute(f"SELECT COUNT(*) as count FROM {table_name}")
                result = cur.fetchone()
                record_count = result['count']
                print(f"  - 레코드 수: {record_count:,}개")
                
                if record_count > 0:
                    # 최신 데이터 확인
                    cur.execute(f"""
                        SELECT created_at 
                        FROM {table_name} 
                        ORDER BY created_at DESC 
                        LIMIT 1
                    """)
                    result = cur.fetchone()
                    if result:
                        print(f"  - 최신 데이터: {result['created_at']}")
                    
                    # 컬럼 정보 확인
                    cur.execute(f"""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_name = '{table_name}'
                        ORDER BY ordinal_position
                    """)
                    columns = cur.fetchall()
                    column_names = [col['column_name'] for col in columns]
                    print(f"  - 컬럼 수: {len(column_names)}개")
                    print(f"  - 컬럼명: {', '.join(column_names[:5])}{'...' if len(column_names) > 5 else ''}")
                
    except Exception as e:
        print(f"❌ 신규 테이블 분석 중 오류 발생: {e}")
    finally:
        conn.close()


def analyze_regions_table():
    """regions 테이블 분석"""
    print("\n\n=== 지역 코드 테이블 분석 ===")
    
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # 테이블 존재 확인
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'regions'
                )
            """)
            
            result = cur.fetchone()
            table_exists = result['exists']
            
            if not table_exists:
                print("⚠️ regions 테이블이 존재하지 않습니다.")
                return
            
            # 전체 지역 수
            cur.execute("SELECT COUNT(*) as total_count FROM regions")
            result = cur.fetchone()
            total_count = result['total_count']
            print(f"총 지역 코드 수: {total_count:,}개")
            
            if total_count > 0:
                # 레벨별 분포
                cur.execute("""
                    SELECT 
                        region_level,
                        COUNT(*) as count
                    FROM regions 
                    GROUP BY region_level 
                    ORDER BY region_level
                """)
                
                levels = cur.fetchall()
                print(f"\n📊 지역 레벨별 분포:")
                for row in levels:
                    level_name = "시도" if row['region_level'] == 1 else "시군구" if row['region_level'] == 2 else f"레벨{row['region_level']}"
                    print(f"  - {level_name}: {row['count']:,}개")
                
                # 시도별 시군구 수
                if total_count > 17:  # 시군구가 있는 경우
                    cur.execute("""
                        SELECT 
                            p.region_name as province_name,
                            COUNT(d.region_code) as district_count
                        FROM regions p
                        LEFT JOIN regions d ON p.region_code = d.parent_region_code
                        WHERE p.region_level = 1
                        GROUP BY p.region_code, p.region_name
                        ORDER BY district_count DESC
                    """)
                    
                    provinces = cur.fetchall()
                    print(f"\n🗺️ 시도별 시군구 수:")
                    for row in provinces:
                        print(f"  - {row['province_name']}: {row['district_count']}개")
    
    except Exception as e:
        print(f"❌ regions 테이블 분석 중 오류 발생: {e}")
    finally:
        conn.close()


def analyze_other_tourism_tables():
    """기타 관광 데이터 테이블 분석"""
    print("\n\n=== 기타 관광 데이터 테이블 분석 ===")
    
    tourism_tables = [
        'tourist_attractions',
        'cultural_facilities', 
        'festivals_events',
        'travel_courses',
        'leisure_sports',
        'accommodations',
        'shopping',
        'restaurants'
    ]
    
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            for table_name in tourism_tables:
                try:
                    # 테이블 존재 확인
                    cur.execute(f"""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_name = '{table_name}'
                        )
                    """)
                    
                    result = cur.fetchone()
                    table_exists = result['exists']
                    
                    if table_exists:
                        # 레코드 수 조회
                        cur.execute(f"SELECT COUNT(*) as count FROM {table_name}")
                        result = cur.fetchone()
                        record_count = result['count']
                        print(f"📋 {table_name}: {record_count:,}개")
                    
                except Exception as e:
                    print(f"📋 {table_name}: 확인 실패 ({e})")
                
    except Exception as e:
        print(f"❌ 기타 테이블 분석 중 오류 발생: {e}")
    finally:
        conn.close()


def main():
    """메인 함수"""
    print("🔍 데이터베이스 원본 데이터 분석")
    print("=" * 60)
    print(f"분석 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # 1. API 원본 데이터 분석
        analyze_api_raw_data()
        
        # 2. 신규 API 테이블 분석
        analyze_new_api_tables()
        
        # 3. 지역 코드 테이블 분석
        analyze_regions_table()
        
        # 4. 기타 관광 데이터 테이블 분석
        analyze_other_tourism_tables()
        
    except Exception as e:
        print(f"❌ 전체 분석 실패: {e}")
    
    print("\n" + "=" * 60)
    print("✅ 데이터베이스 분석 완료")


if __name__ == "__main__":
    main()