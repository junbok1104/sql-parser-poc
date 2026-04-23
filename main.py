import json
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

if __name__ == "__main__":
    # results = run_pipeline()
    # save_parsed_logs(results)
    run_analysis()
