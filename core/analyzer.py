import math
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

    def extract_join_key(self, on_condition):
        """'LH.LOT_ID = MD.LOT_ID' -> 'LOT_ID' 추출 로직"""
        if not on_condition: return ""
        # 등호나 AND/OR 기준으로 분리
        parts = re.split(r'=|AND|OR', on_condition.upper())
        for p in parts:
            # 별칭(A.) 제거하고 순수 컬럼명만 추출
            clean = p.split('.')[-1].strip()
            if clean and not clean.isnumeric(): # 숫자가 아닌 첫 번째 단어를 키로 간주
                return clean
        return ""

    def generate_graph_data(self, meta_list):
        nodes = {}
        edges = {}  # {(src, dst): {"count": 0, "key": ""}}

        for meta in meta_list:
            tables = meta.get('tables', [])
            joins = meta.get('joins', [])
            alias_map = meta.get('alias_map', {})

            # 1. 노드 정보 수집 (물리/가상 구분 및 로그 스케일 크기)
            for t in tables:
                f_path = t['full_path']
                if f_path not in nodes:
                    nodes[f_path] = {"label": t['short_name'], "count": 0, "full_path": f_path}
                nodes[f_path]["count"] += 1

            # 2. 정확한 JOIN 파트너 및 조인 키 추출
            for j in joins:
                target_table = j.get('full_path')
                condition = j.get('on', '')
                source_table = None

                # ON 조건에서 별칭 추출 및 파트너 식별
                found_aliases = re.findall(r'(\w+)\.', condition)
                for alias in found_aliases:
                    actual_path = alias_map.get(alias.upper())
                    if actual_path and actual_path != target_table:
                        source_table = actual_path
                        break

                if not source_table and tables:
                    source_table = tables[0]['full_path']

                if source_table and target_table and source_table != target_table:
                    pair = tuple(sorted([source_table, target_table]))
                    join_key = self.extract_join_key(condition)

                    if pair not in edges:
                        edges[pair] = {"count": 0, "key": join_key, "raw_on": condition}
                    edges[pair]["count"] += 1

            # 3. 시각화 데이터 포맷팅 (레퍼런스 스타일 적용)
            formatted_nodes = []
            for f_path, info in nodes.items():
                is_physical = "." in f_path
                # 로그 스케일로 크기 균형 조정
                size = 15 + (math.log(info['count'] + 1) * 8)

                formatted_nodes.append({
                    "id": f_path,
                    "label": info['label'], # HTML 라벨 지원
                    "size": size,
                    "shape": "box" if is_physical else "ellipse", # 물리 테이블은 박스형
                    "color": {
                        "background": "#FFFFFF",
                        "border": "#1A73E8" if is_physical else "#9aa0a6",
                        "highlight": "#E8F0FE"
                    },
                    "font": {"multi": "html", "size": 14},
                    "title": f"Path: {f_path}\nReferences: {info['count']}"
                })

            formatted_edges = []
            for p, data in edges.items():
                # 선 위에 "조인키 (횟수)" 표시 -> 레퍼런스 이미지 스타일
                label_text = f"{data['key']}\n({data['count']}회)" if data['key'] else f"JOIN\n({data['count']}회)"

                formatted_edges.append({
                    "from": p[0],
                    "to": p[1],
                    "value": data['count'], # 빈도에 따른 선 굵기
                    "label": label_text,
                    "title": f"Condition: {data['raw_on']}\nTotal: {data['count']} times",
                    "color": {"color": "#1A73E8", "opacity": 0.5},
                    "font": {"size": 10, "align": "middle", "background": "#ffffff"}
                })

        # 4. 분석 결과 리포팅
        self._print_analysis_report(nodes, edges, len(meta_list))

        return {"nodes": formatted_nodes, "edges": formatted_edges}

    def _print_analysis_report(self, nodes, edges, total_count):
        """분석 결과를 터미널에 깔끔하게 출력합니다."""
        print(f"\n📊 분석 대상 데이터: {total_count}건")

        print("\n--- 분석 결과 (Nodes: 인기도 순) ---")
        # 노드는 info['count'] 기준으로 정렬
        for f_path, info in sorted(nodes.items(), key=lambda x: x[1]['count'], reverse=True):
            print(f"📍 {info['label']} (참조: {info['count']}회)")

        print("\n--- 분석 결과 (Edges: 결합도 순) ---")
        # [수정 포인트] x[1]이 딕셔너리이므로 x[1]['count']를 기준으로 정렬해야 에러가 안 납니다.
        for p, data in sorted(edges.items(), key=lambda x: x[1]['count'], reverse=True):
            # p는 (source, target) 튜플입니다.
            src_short = p[0].split('.')[-1]
            dst_short = p[1].split('.')[-1]
            print(f"🔗 {src_short} <-> {dst_short} (Join: {data['count']}회, Key: {data['key']})")

    def visualize_lineage(self, data_input, output_filename="output/lineage_map.html"):
        if isinstance(data_input, list):
            graph_data = self.generate_graph_data(data_input)
        else:
            graph_data = data_input

        os.makedirs(os.path.dirname(output_filename), exist_ok=True)

        # 1. 밝은 테마로 설정 (레퍼런스 이미지 스타일)
        net = Network(height="850px", width="100%", bgcolor="#ffffff", font_color="#333333", notebook=False)

        # 2. 노드 추가 (font multi 옵션 필수!)
        if 'nodes' in graph_data:
            for node in graph_data['nodes']:
                net.add_node(
                    node['id'],
                    label=node['label'], # 이제 순수 텍스트만 들어감
                    title=node.get('title', ""),
                    color=node.get('color', "#1A73E8"),
                    size=node.get('size', 20),
                    shape="box",
                    # font 설정을 태그 없이 굵고 깔끔하게 지정
                    font={
                        "size": 15,
                        "face": "Arial Black", # 폰트 자체를 굵은 녀석으로 지정하는 게 가장 확실합니다.
                        "color": "#333333"
                    }
                )

        # 3. 엣지 추가
        if 'edges' in graph_data:
            for edge in graph_data['edges']:
                net.add_edge(
                    edge['from'],
                    edge['to'],
                    value=edge.get('value', 1),
                    label=edge.get('label', ""),
                    title=edge.get('title', ""),
                    color={"color": "#1A73E8", "opacity": 0.3}, # 선을 연하게 해서 글자가 잘 보이게
                    font={"size": 11, "align": "middle", "background": "rgba(255, 255, 255, 0.7)", "strokeWidth": 0}
                )

        # 4. 물리 엔진 최적화 (서로 너무 겹치지 않게 거리 조정)
        net.set_options("""
            var options = {
              "physics": {
                "forceAtlas2Based": {
                  "gravitationalConstant": -200,
                  "centralGravity": 0.01,
                  "springLength": 250,
                  "springConstant": 0.08
                },
                "solver": "forceAtlas2Based",
                "stabilization": { "iterations": 150 }
              }
            }
        """)

        net.save_graph(output_filename)
        print(f"🎨 시각화 완료: {output_filename} 확인")