import sqlglot
from sqlglot import exp
import re

class SQLParser:
    def __init__(self):
        # 별칭 제거용 정규식 (lh.ID -> ID)
        self.alias_pattern = re.compile(r'\b\w+\.')

    def parse_query(self, sql):
        try:
            # 1. SQL 파싱 (다양한 방언 대응을 위해 필요시 dialect 설정 가능)
            expression = sqlglot.parse_one(sql)

            # 2. 테이블 정보 추출 (Full Path와 Short Name 병행)
            table_info = []
            seen_full_paths = set()

            for table_expr in expression.find_all(exp.Table):
                # Full Path: LAKE_CATALOG.MDM.LOT_HIST
                # full_path = table_expr.sql(identify=False).upper()
                full_path = table_expr.sql(identify=False).split(' AS ')[0].upper()

                # 중복 방지
                if full_path not in seen_full_paths:
                    # Short Name: LOT_HIST
                    short_name = table_expr.this.sql(identify=False).upper()

                    table_info.append({
                        "full_path": full_path,
                        "short_name": short_name
                    })
                    seen_full_paths.add(full_path)

            # 3. 조인 조건 추출
            joins = []
            for join in expression.find_all(exp.Join):
                # 1. Join 대상 테이블 경로 추출
                j_full = join.this.sql(identify=False).upper().split(' ')[0]
                j_short = j_full.split('.')[-1]

                # 2. ON 조건 추출 (별칭을 지우지 마세요!)
                # self._clean_alias를 거치지 않고 원본 sql을 가져오거나,
                # 단순히 대문자 변환만 수행합니다.
                on_condition = join.args.get('on').sql(identify=False).upper() if join.args.get('on') else ""

                joins.append({
                    "full_path": j_full,
                    "short_name": j_short,
                    "on": on_condition  # <--- 별칭(A., B.)이 살아있는 상태
                })

            # 4. SELECT 절 컬럼 추출 (interests 지표)
            # column.find_ancestor(exp.Select)를 쓰면 더 정확하게 SELECT 소속인지 파악 가능합니다.
            interests = list(set([
                col.sql(identify=False).upper()
                for col in expression.find_all(exp.Column)
                if col.find_ancestor(exp.Select) and not col.find_ancestor(exp.Where)
            ]))

            # 5. WHERE 절 컬럼 + 리터럴 값 추출 (hotFilters)
            hot_filters = []
            for condition in expression.find_all(exp.Binary): # =, <, >, != 등
                if condition.find_ancestor(exp.Where):
                    left = condition.left.sql(identify=False).upper()
                    right = condition.right.sql(identify=False) # 값은 대문자 변환 없이
                    hot_filters.append(f"{left} {condition.key} {right}")

            # 6. GROUP / ORDER BY 추출 (alsoUsedIn: ['AGG'])
            aggregations = []
            for agg in expression.find_all(exp.Group, exp.Order):
                aggregations.extend([
                    node.sql(identify=False).upper()
                    for node in agg.find_all(exp.Column)
                ])
            aggregations = list(set(aggregations))

            # 7. Alias Map 추출
            # {"MD": "LAKE_CATALOG.MDM.MEASUREMENT_DATA", "RD": "RECENT_LOTS"}
            alias_map = {}
            for table in expression.find_all(exp.Table):
                f_path = table.sql(identify=False).split(' AS ')[0].upper()
                alias = table.alias.upper() if table.alias else None
                if alias:
                    alias_map[alias] = f_path

            return {
                "tables": table_info,
                "joins": joins,
                "alias_map": alias_map,
                "interests": interests,        # SELECT 컬럼
                "hotFilters": hot_filters,     # WHERE 조건
                "aggregations": aggregations   # GROUP/ORDER BY 컬럼
            }
        except Exception as e:
            print(f"❌ 파싱 에러: {e}")
            return None


    def _clean_alias(self, condition):
        if not condition: return ""
        return self.alias_pattern.sub('', condition).upper()

    def extract_alias_map(expression):
        alias_map = {}
        # 쿼리 내 모든 Table 노드를 찾아서 별칭이 있는지 확인
        for table in expression.find_all(exp.Table):
            table_full_path = table.sql() # 전체 경로
            alias = table.alias
            if alias:
                alias_map[alias] = table_full_path
        return alias_map