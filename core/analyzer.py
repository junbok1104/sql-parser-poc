import os
import re
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
        import re
        from collections import Counter

        nodes = {}  # {full_path: {"label": short_name, "count": 0}}
        edges = Counter()  # {(source, target): weight}
        edge_labels = {}   # {pair: condition}

        for meta in meta_list:
            tables = meta.get('tables', [])
            joins = meta.get('joins', [])
            # [핵심] 파서가 제공하는 정확한 별칭 맵 (예: {"MD": "LAKE_CATALOG.MDM.MEASUREMENT_DATA"})
            alias_map = meta.get('alias_map', {})

            # 1. 노드 정보 및 참조 횟수 수집
            for t in tables:
                f_path = t['full_path']
                if f_path not in nodes:
                    nodes[f_path] = {"label": t['short_name'], "count": 0}
                nodes[f_path]["count"] += 1

            # 2. JOIN 파트너 결정
            for j in joins:
                target_table = j.get('full_path')
                condition = j.get('on', '')
                source_table = None

                # [Step 1] ON 조건에서 별칭 추출 (예: "MD.ID = RD.ID" -> ["MD", "RD"])
                found_aliases = re.findall(r'(\w+)\.', condition)

                # [Step 2] 추출된 별칭 중 target_table이 아닌 '상대방' 테이블 찾기
                for alias in found_aliases:
                    actual_path = alias_map.get(alias)
                    # 별칭이 가리키는 실제 경로가 존재하고, 그게 현재 JOIN 대상이 아니라면 그놈이 source다!
                    if actual_path and actual_path != target_table:
                        source_table = actual_path
                        break

                # [Step 3] Alias로 못 찾았을 경우의 Fallback (여전히 필요)
                if not source_table and tables:
                    source_table = tables[0]['full_path']

                # [Step 4] 엣지 확정
                if source_table and target_table and source_table != target_table:
                    pair = tuple(sorted([source_table, target_table]))
                    edges[pair] += 1
                    edge_labels[pair] = condition

        # 3. 시각화 데이터 포맷팅
        formatted_nodes = []
        for f_path, info in nodes.items():
            size = 15 + (info['count'] * 3)
            # 참조가 많은 핵심 테이블은 색상을 다르게
            color = "#1A73E8" if info['count'] > 10 else "#8AB4F8"
            formatted_nodes.append({
                "id": f_path,
                "label": info['label'],
                "size": size,
                "color": color,
                "title": f"Path: {f_path}\nTotal References: {info['count']}"
            })

        formatted_edges = [
            {
                "from": p[0],
                "to": p[1],
                "value": count,
                "title": f"Condition: {edge_labels.get(p, '')}\nTotal Joins: {count}",
                "label": edge_labels.get(p, '') if count > 2 else "" # 빈도가 높은 조인만 라벨 노출
            }
            for p, count in edges.items()
        ]

        # 4. 분석 결과 리포팅 (터미널)
        self._print_analysis_report(nodes, edges, len(meta_list))

        return {"nodes": formatted_nodes, "edges": formatted_edges}

    def _print_analysis_report(self, nodes, edges, total_count):
        """분석 결과를 터미널에 깔끔하게 출력합니다."""
        print(f"\n📊 분석 대상 데이터: {total_count}건")
        print("\n--- 분석 결과 (Nodes: 인기도 순) ---")
        for f_path, info in sorted(nodes.items(), key=lambda x: x[1]['count'], reverse=True):
            print(f"📍 {info['label']} (참조: {info['count']}회)")

        print("\n--- 분석 결과 (Edges: 결합도 순) ---")
        for p, count in sorted(edges.items(), key=lambda x: x[1], reverse=True):
            print(f"🔗 {p[0].split('.')[-1]} <-> {p[1].split('.')[-1]} (Join: {count}회)")

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