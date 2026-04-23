import os
from collections import Counter

from pyvis.network import Network
from sqlalchemy import text
from database.connection import engine

class SQLAnalyzer:
    def __init__(self):
        pass

    def fetch_parsed_data(self):
        """DB에서 파싱된 JSON 데이터를 긁어옵니다."""
        with engine.connect() as conn:
            query = text("SELECT parsed_meta FROM lake_public.sql_query_status WHERE status = 'PARSED'")
            result = conn.execute(query)
            return [row[0] for row in result]

    def analyze_relationships(self, meta_list):
        """테이블 간 JOIN 빈도를 분석합니다. (SQL 3.1)"""
        join_counts = Counter()

        for meta in meta_list:
            # 5년 차의 센스: Full Path 기준으로 집계
            tables = [t['full_path'] for t in meta.get('tables', [])]

            # 한 쿼리에 등장한 테이블들끼리의 조합 생성 (간단한 예시)
            if len(tables) > 1:
                # 테이블 쌍을 정렬해서 A-B와 B-A가 같은 것으로 취급되게 함
                for i in range(len(tables)):
                    for j in range(i + 1, len(tables)):
                        pair = tuple(sorted([tables[i], tables[j]]))
                        join_counts[pair] += 1

        return join_counts

    def calculate_table_stats(self, meta_list):
        """테이블별 등장 빈도 및 핵심도 계산 (SQL 3.2)"""
        table_counts = Counter()
        column_counts = Counter()

        for meta in meta_list:
            # 테이블 등장 횟수
            for t in meta.get('tables', []):
                table_counts[t['full_path']] += 1

            # 조인에 사용된 컬럼 스코어링 (가중치 부여 가능)
            for j in meta.get('joins', []):
                # 예: JOIN 조건에 나온 컬럼은 중요도가 높음
                col = j.get('on', '')
                if col:
                    column_counts[col] += 2 # 조인 컬럼은 가중치 2점

        return {
            "top_tables": table_counts.most_common(10),
            "top_columns": column_counts.most_common(10)
        }

    def generate_graph_data(self, meta_list):
        """시각화용 노드와 엣지 데이터를 생성합니다. (JOIN 기반 정교화)"""
        nodes = {}  # {full_path: short_name}
        edges = Counter()  # {(source, target): weight}
        edge_labels = {}   # {(source, target): join_condition} - 선 위에 표시할 텍스트

        for meta in meta_list:
            tables = meta.get('tables', [])
            joins = meta.get('joins', []) # 5년 차의 핵심: joins 데이터 활용

            # 1. 노드 등록 (기존과 동일)
            for t in tables:
                f_path = t['full_path']
                if f_path not in nodes:
                    nodes[f_path] = t['short_name']

            # 2. 엣지(Join 관계) 등록 (여기서 거미줄을 제거합니다!)
            # 기존의 이중 for문을 제거하고, 실제 join 리스트를 기반으로 생성합니다.

            # 메인 테이블 선정 (보통 tables의 첫 번째가 FROM 절의 메인 테이블)
            if tables:
                main_table_path = tables[0]['full_path']

                for j in joins:
                    target_table_path = j.get('full_path')
                    join_on = j.get('on', '')

                    if target_table_path and main_table_path != target_table_path:
                        # 정렬된 튜플로 키 생성 (무방향 그래프 중복 방지)
                        pair = tuple(sorted([main_table_path, target_table_path]))
                        edges[pair] += 1

                        # 선 위에 표시할 ON 조건 저장 (마지막 조건으로 덮어쓰거나 누적)
                        edge_labels[pair] = join_on

        # 시각화 라이브러리에 전달하기 좋은 형태로 포맷팅
        formatted_nodes = [{"id": f, "label": s} for f, s in nodes.items()]
        formatted_edges = [
            {
                "from": p[0],
                "to": p[1],
                "value": count,
                "title": f"Condition: {edge_labels.get(p, '')}\nTotal Joins: {count}", # 툴팁에 ON 조건 추가
                "label": edge_labels.get(p, '') if count > 0 else "" # 선 위에 직접 라벨 표시
            }
            for p, count in edges.items()
        ]

        return {"nodes": formatted_nodes, "edges": formatted_edges}


    def visualize_lineage(self, graph_data, output_filename="output/lineage_map.html"):
        """분석된 데이터를 바탕으로 HTML 시각화 파일을 생성합니다."""
        # 디렉토리가 없으면 생성
        os.makedirs(os.path.dirname(output_filename), exist_ok=True)

        # 네트워크 객체 생성 (배경색, 폰트 등 설정)
        net = Network(height="750px", width="100%", bgcolor="#222222", font_color="white", notebook=False)

        # 1. 노드 추가
        for node in graph_data['nodes']:
            net.add_node(node['id'], label=node['label'], title=node['id'], color="#4285F4")

        # 2. 엣지 추가 (value가 굵기가 됩니다)
        for edge in graph_data['edges']:
            net.add_edge(edge['from'], edge['to'], value=edge['value'], title=f"Join Count: {edge['value']}", color="#FBBC05")

        # 물리 엔진 설정 (노드들이 예쁘게 퍼지도록)
        net.toggle_physics(True)

        # 파일 저장
        net.save_graph(output_filename)
        print(f"🎨 시각화 완료: {output_filename} 확인")