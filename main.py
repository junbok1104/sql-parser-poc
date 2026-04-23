import json
import os
from datetime import datetime
from sqlalchemy import text

from core.analyzer import SQLAnalyzer
from core.parser import SQLParser
from core.sessionizer import Sessionizer
from database.connection import engine

def save_parsed_logs(results):
    """분석된 로그를 lake_public.sql_query_status 테이블에 저장 (SQL 2.4)"""
    with engine.connect() as conn:
        for res in results:
            # DB에서 UUID를 생성하므로 query_id는 제외
            query = text("""
                         INSERT INTO lake_public.sql_query_status
                             (user_id, session_id, raw_query, status, parsed_meta)
                         VALUES (:user_id, :session_id, :raw_query, :status, :parsed_meta)
                         """)

            conn.execute(query, {
                "user_id": res['user_id'],
                "session_id": res['session_id'],
                "raw_query": res['query'],
                "status": "PARSED",
                "parsed_meta": json.dumps(res['parsed_meta']) # JSONB 저장을 위한 변환
            })
        conn.commit()
    print(f"✅ DB 저장 완료: {len(results)}건의 로그가 lake_public에 적재되었습니다.")

def run_pipeline():
    # 1. 샘플 데이터 로드 (SQL 1.1 대응: 실제로는 DB/OpenSearch에서 가져올 데이터)
    raw_logs = [
        {
            "user_id": "junbok_lee",
            "timestamp": datetime(2026, 4, 23, 10, 0),
            "query": "SELECT * FROM lake_catalog.mdm.LOT_HIST lh JOIN lake_catalog.mdm.MEASUREMENT_DATA md ON lh.LOT_ID = md.LOT_ID"
        },
        {
            "user_id": "junbok_lee",
            "timestamp": datetime(2026, 4, 23, 10, 3), # 3분 뒤 실행 (동일 세션)
            "query": "SELECT * FROM lake_catalog.mdm.LOT_HIST WHERE LOT_ID = 'L12345'"
        },
        {
            "user_id": "other_user",
            "timestamp": datetime(2026, 4, 23, 11, 0), # 다른 유저, 다른 시간 (새 세션)
            "query": "SELECT * FROM lake_catalog.mdm.MDM_LFB_PROD_M A JOIN lake_catalog.mdm.MDM_LFB_PRODFLOW_W B ON A.PROD_ID = B.PROD_ID"
        },
        {
        "user_id": "junbok_dev",
        "timestamp": datetime(2026, 4, 23, 14, 20),
        "query": """
                 WITH RECENT_LOTS AS (
                     SELECT
                         LH.LOT_ID,
                         LH.PROD_CODE,
                         LH.STATION_ID,
                         LH.CREATE_DT
                     FROM LAKE_CATALOG.MDM.LOT_HIST AS LH
                     WHERE LH.CREATE_DT >= :start_date
                       AND LH.STATUS IN ('COMPLETED', 'SHIPPED')
                 ),
                      STATS_DATA AS (
                          SELECT
                              RD.PROD_CODE,
                              RD.STATION_ID,
                              AVG(MD.VALUE) OVER (PARTITION BY RD.PROD_CODE) as AVG_VALUE,
                              COUNT(MD.MEASURE_ID) as MEASURE_CNT
                          FROM RECENT_LOTS RD
                                   LEFT JOIN LAKE_CATALOG.MDM.MEASUREMENT_DATA AS MD ON RD.LOT_ID = MD.LOT_ID
                          WHERE MD.VALUE IS NOT NULL
                          GROUP BY RD.PROD_CODE, RD.STATION_ID, MD.VALUE
                      )
                 SELECT
                     A.PROD_CODE,
                     B.PROD_NAME,
                     A.STATION_ID,
                     A.AVG_VALUE,
                     (SELECT COUNT(*) FROM LAKE_CATALOG.MDM.ALARM_HIST WHERE PROD_CODE = A.PROD_CODE) as ALARM_COUNT
                 FROM STATS_DATA A
                          JOIN LAKE_CATALOG.MDM.MDM_LFB_PROD_M AS B ON A.PROD_CODE = B.PROD_CODE
                 WHERE A.MEASURE_CNT > 10
                 ORDER BY A.AVG_VALUE DESC
                 """
        },
        # Case 1: 바인드 변수 (:param) 및 복잡한 조건식
        {
            "user_id": "tester_01",
            "timestamp": datetime(2026, 4, 23, 15, 0),
            "query": "SELECT * FROM LAKE_CATALOG.MDM.LOT_HIST WHERE CREATE_DT BETWEEN :START_DT AND :END_DT OR STATUS IS NULL"
        },

        # Case 2: CTE (WITH절) - 가상 테이블 인식 한계 테스트
        {
            "user_id": "tester_02",
            "timestamp": datetime(2026, 4, 23, 15, 5),
            "query": "WITH TEMP_VIEW AS (SELECT LOT_ID FROM LAKE_CATALOG.MDM.LOT_HIST) SELECT * FROM TEMP_VIEW"
        },

        # Case 3: UNION ALL - 다중 쿼리 결합 시 테이블 추출 테스트
        {
            "user_id": "tester_03",
            "timestamp": datetime(2026, 4, 23, 15, 10),
            "query": "SELECT PROD_ID FROM LAKE_CATALOG.MDM.MDM_LFB_PROD_M UNION ALL SELECT PROD_ID FROM LAKE_CATALOG.MDM.MDM_LFB_PRODFLOW_W"
        },

        # Case 4: 서브쿼리 (Inline View) - 계층적 구조 파싱 테스트
        {
            "user_id": "tester_04",
            "timestamp": datetime(2026, 4, 23, 15, 15),
            "query": "SELECT * FROM (SELECT LOT_ID, VALUE FROM LAKE_CATALOG.MDM.MEASUREMENT_DATA) A WHERE A.VALUE > 10"
        },

        # Case 5: 멀티라인 주석 및 사내 UDF 호출 - 문법 방해 요소 테스트
        {
            "user_id": "tester_05",
            "timestamp": datetime(2026, 4, 23, 15, 20),
            "query": """
                     /* 작성자: 이준복
                        목적: 사내 특화 함수 테스트 
                     */
                     SELECT FN_CUSTOM_FORMAT(LOT_ID) as FORMATTED_ID
                     FROM LAKE_CATALOG.MDM.LOT_HIST
                     WHERE STATUS = 'A'
                     """
        },
        {
            "user_id": "tester_union",
            "timestamp": datetime(2026, 4, 23, 15, 30),
            "query": """
                     /* 생산 마스터와 공정 마스터 정보를 통합 조회 (UNION ALL 테스트) */
                     SELECT
                         PROD_ID,
                         PROD_NAME,
                         'MASTER' as SRC_TYPE
                     FROM LAKE_CATALOG.MDM.MDM_LFB_PROD_M
                     WHERE STATUS = 'A'

                     UNION ALL

                     SELECT
                         PROD_ID,
                         FLOW_NAME as PROD_NAME,
                         'FLOW' as SRC_TYPE
                     FROM LAKE_CATALOG.MDM.MDM_LFB_PRODFLOW_W
                     WHERE USE_YN = 'Y'
                     """
        }
    ]

    print("🚀 Ash SQL Parser Pipeline 시작...")

    # 2. 세션 그룹핑 (SQL 2.2)
    sessionizer = Sessionizer(timeout_minutes=5)
    sessionized_logs = sessionizer.group_by_session(raw_logs)

    # 3. SQL 파싱 및 메타데이터 추출 (SQL 2.3)
    parser = SQLParser()

    final_results = []
    for log in sessionized_logs:
        parsed_meta = parser.parse_query(log['query'])
        if parsed_meta:
            log['parsed_meta'] = parsed_meta
            final_results.append(log)

    # 4. 결과 출력 (산출물 확인용)
    print(f"\n✅ 분석 완료: 총 {len(final_results)}건의 로그 처리됨")
    for res in final_results:
        print(f"[{res['session_id']}] {res['user_id']}: {res['parsed_meta']['tables']} 추출 완료")

    return final_results # 결과 리턴

def run_analysis():
    analyzer = SQLAnalyzer()

    # 1. DB에서 데이터 가져오기
    meta_list = analyzer.fetch_parsed_data()
    print(f"📊 분석 대상 데이터: {len(meta_list)}건")

    # 2. 그래프 데이터 생성
    graph_data = analyzer.generate_graph_data(meta_list)

    print("\n--- 분석 결과 (Nodes) ---")
    for node in graph_data['nodes']:
        print(f"📍 {node['label']} ({node['id']})")

    print("\n--- 분석 결과 (Edges) ---")
    for edge in graph_data['edges']:
        print(f"🔗 {edge['from']} <-> {edge['to']} (Join 횟수: {edge['value']})")

    # 시각화
    analyzer.visualize_lineage(graph_data)

def run_parsed_data_test(parsed_list):
    """
    이미 파싱이 완료된 데이터 리스트를 받아 즉시 시각화합니다.
    """
    analyzer = SQLAnalyzer()

    if not parsed_list:
        print("⚠️ 분석할 데이터가 없습니다.")
        return

    print(f"📊 이미 파싱된 {len(parsed_list)}건의 데이터를 시각화합니다.")

    try:
        # 핵심: analyzer 내부에 이미 정의된 visualize_lineage를 호출하되,
        # 내부에서 generate_graph_data(meta_list)를 호출하는 구조라면
        # 아래처럼 parsed_list(meta_list의 형태)를 그대로 넘겨주면 됩니다.
        analyzer.visualize_lineage(parsed_list)

        print("\n✨ 시각화 완료! output/lineage_map.html 파일을 확인하세요.")

    except Exception as e:
        print(f"❌ 시각화 생성 중 오류 발생: {e}")
        # 만약 위에서 에러가 난다면, analyzer가 내부적으로 데이터를 처리하는 방식에
        # 차이가 있을 수 있으니 아래처럼 직접 호출해볼 수도 있습니다.
        # graph_data = analyzer.generate_graph_data(parsed_list)
        # analyzer.save_html(graph_data)

if __name__ == "__main__":
    # results = run_pipeline()
    # save_parsed_logs(results)
    # run_analysis()

    import json

    # DB가 없는 사용자를 위해 즉시 시각화 가능한 데이터 세트
    parsed_sample_data = [
        # 1. 가장 복잡한 메인 분석 쿼리 (CTE + Join + Subquery)
        {
            "joins": [{"on": "A.PROD_CODE = B.PROD_CODE", "full_path": "LAKE_CATALOG.MDM.MDM_LFB_PROD_M", "short_name": "MDM_LFB_PROD_M"}, {"on": "RD.LOT_ID = MD.LOT_ID", "full_path": "LAKE_CATALOG.MDM.MEASUREMENT_DATA", "short_name": "MEASUREMENT_DATA"}],
            "tables": [{"full_path": "STATS_DATA", "short_name": "STATS_DATA"}, {"full_path": "LAKE_CATALOG.MDM.MDM_LFB_PROD_M", "short_name": "MDM_LFB_PROD_M"}, {"full_path": "LAKE_CATALOG.MDM.ALARM_HIST", "short_name": "ALARM_HIST"}, {"full_path": "LAKE_CATALOG.MDM.LOT_HIST", "short_name": "LOT_HIST"}, {"full_path": "RECENT_LOTS", "short_name": "RECENT_LOTS"}, {"full_path": "LAKE_CATALOG.MDM.MEASUREMENT_DATA", "short_name": "MEASUREMENT_DATA"}],
            "alias_map": {"A": "STATS_DATA", "B": "LAKE_CATALOG.MDM.MDM_LFB_PROD_M", "LH": "LAKE_CATALOG.MDM.LOT_HIST", "MD": "LAKE_CATALOG.MDM.MEASUREMENT_DATA", "RD": "RECENT_LOTS"},
            "interests": ["LH.LOT_ID", "RD.STATION_ID", "A.AVG_VALUE", "MD.LOT_ID", "LH.STATION_ID", "B.PROD_CODE", "LH.PROD_CODE", "MD.MEASURE_ID", "A.STATION_ID", "LH.CREATE_DT", "A.PROD_CODE", "MD.VALUE", "RD.PROD_CODE", "RD.LOT_ID", "B.PROD_NAME"],
            "hotFilters": ["A.MEASURE_CNT gt 10", "PROD_CODE eq A.PROD_CODE", "LH.CREATE_DT >= :START_DATE and LH.STATUS IN ('COMPLETED', 'SHIPPED')", "LH.CREATE_DT gte :start_date", "MD.VALUE is NULL"],
            "aggregations": ["MD.VALUE", "RD.PROD_CODE", "RD.STATION_ID", "A.AVG_VALUE"]
        },
        # 2. 기본 조인 (LH <-> MD)
        {
            "joins": [{"on": "LH.LOT_ID = MD.LOT_ID", "full_path": "LAKE_CATALOG.MDM.MEASUREMENT_DATA", "short_name": "MEASUREMENT_DATA"}],
            "tables": [{"full_path": "LAKE_CATALOG.MDM.LOT_HIST", "short_name": "LOT_HIST"}, {"full_path": "LAKE_CATALOG.MDM.MEASUREMENT_DATA", "short_name": "MEASUREMENT_DATA"}],
            "alias_map": {"LH": "LAKE_CATALOG.MDM.LOT_HIST", "MD": "LAKE_CATALOG.MDM.MEASUREMENT_DATA"},
            "interests": ["LH.LOT_ID", "MD.LOT_ID"], "hotFilters": [], "aggregations": []
        },
        # 3. 단일 테이블 조회 (Filter 테스트)
        {
            "joins": [], "tables": [{"full_path": "LAKE_CATALOG.MDM.LOT_HIST", "short_name": "LOT_HIST"}],
            "alias_map": {}, "interests": [], "hotFilters": ["LOT_ID eq 'L12345'"], "aggregations": []
        },
        # 4. 마스터 데이터 조인 (A <-> B)
        {
            "joins": [{"on": "A.PROD_ID = B.PROD_ID", "full_path": "LAKE_CATALOG.MDM.MDM_LFB_PRODFLOW_W", "short_name": "MDM_LFB_PRODFLOW_W"}],
            "tables": [{"full_path": "LAKE_CATALOG.MDM.MDM_LFB_PROD_M", "short_name": "MDM_LFB_PROD_M"}, {"full_path": "LAKE_CATALOG.MDM.MDM_LFB_PRODFLOW_W", "short_name": "MDM_LFB_PRODFLOW_W"}],
            "alias_map": {"A": "LAKE_CATALOG.MDM.MDM_LFB_PROD_M", "B": "LAKE_CATALOG.MDM.MDM_LFB_PRODFLOW_W"},
            "interests": ["B.PROD_ID", "A.PROD_ID"], "hotFilters": [], "aggregations": []
        },
        # 5. 복합 조건절 테스트
        {
            "joins": [], "tables": [{"full_path": "LAKE_CATALOG.MDM.LOT_HIST", "short_name": "LOT_HIST"}],
            "alias_map": {}, "interests": [], "hotFilters": ["CREATE_DT BETWEEN :START_DT AND :END_DT or STATUS IS NULL", "STATUS is NULL"], "aggregations": []
        },
        # 6. WITH절 (CTE) 가상 테이블 테스트
        {
            "joins": [], "tables": [{"full_path": "TEMP_VIEW", "short_name": "TEMP_VIEW"}, {"full_path": "LAKE_CATALOG.MDM.LOT_HIST", "short_name": "LOT_HIST"}],
            "alias_map": {}, "interests": ["LOT_ID"], "hotFilters": [], "aggregations": []
        },
        # 7. UNION ALL (다중 테이블 추출)
        {
            "joins": [], "tables": [{"full_path": "LAKE_CATALOG.MDM.MDM_LFB_PROD_M", "short_name": "MDM_LFB_PROD_M"}, {"full_path": "LAKE_CATALOG.MDM.MDM_LFB_PRODFLOW_W", "short_name": "MDM_LFB_PRODFLOW_W"}],
            "alias_map": {}, "interests": ["PROD_ID"], "hotFilters": [], "aggregations": []
        },
        # 8. 서브쿼리 내 필터 테스트
        {
            "joins": [], "tables": [{"full_path": "LAKE_CATALOG.MDM.MEASUREMENT_DATA", "short_name": "MEASUREMENT_DATA"}],
            "alias_map": {}, "interests": ["VALUE", "LOT_ID"], "hotFilters": ["A.VALUE gt 10"], "aggregations": []
        },
        # 9. 사용자 정의 함수/주석 포함 쿼리
        {
            "joins": [], "tables": [{"full_path": "LAKE_CATALOG.MDM.LOT_HIST", "short_name": "LOT_HIST"}],
            "alias_map": {}, "interests": ["LOT_ID"], "hotFilters": ["STATUS eq 'A'"], "aggregations": []
        },
        # 10. 복합 UNION ALL 마스터 정보
        {
            "joins": [], "tables": [{"full_path": "LAKE_CATALOG.MDM.MDM_LFB_PROD_M", "short_name": "MDM_LFB_PROD_M"}, {"full_path": "LAKE_CATALOG.MDM.MDM_LFB_PRODFLOW_W", "short_name": "MDM_LFB_PRODFLOW_W"}],
            "alias_map": {}, "interests": ["PROD_ID", "FLOW_NAME", "PROD_NAME"], "hotFilters": ["STATUS eq 'A'", "USE_YN eq 'Y'"], "aggregations": []
        }
    ]
    run_parsed_data_test(parsed_sample_data)
