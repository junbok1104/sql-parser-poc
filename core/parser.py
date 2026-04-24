import sqlglot
from sqlglot import exp
import re

class SQLParser:
    def __init__(self):
        # 시나리오별 우선순위 방언 리스트
        # 1. Databricks/Spark (LAKE_CATALOG 대응)
        # 2. Oracle (바인드 변수 :1 대응)
        # 3. Postgres/MySQL (표준 및 일반 SQL)
        # 4. None (표준 SQL)
        self.dialects = ["databricks", "oracle", "postgres", "mysql", None]

    def _parse_with_fallback(self, sql):
        """전처리를 포함하여 다양한 방언으로 파싱 시도"""
        if not sql: return None

        # 1. 전처리: :1, :2 패턴을 파서가 인식하기 쉬운 형태로 임시 치환
        # (일부 방언에서 콜론+숫자를 문법 에러로 처리하는 것 방지)
        processed_sql = re.sub(r':(\d+)', r'__BIND_\1', sql)

        # 2. 방언 루프 실행
        for dialect in self.dialects:
            try:
                # 시도할 때 전처리된 SQL과 원본 SQL 두 번 시도하면 더 안전합니다.
                for s in [processed_sql, sql]:
                    try:
                        return sqlglot.parse_one(s, read=dialect)
                    except:
                        continue
            except Exception:
                continue

        return None

    def parse_query(self, sql):
        try:
            # 1. SQL 파싱 - 다중 방언 시도 (에러 발생 시 None 반환)
            expression = self._parse_with_fallback(sql)

            if not expression:
                raise ValueError("모든 지원 방언으로 파싱에 실패했습니다.")

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