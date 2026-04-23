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
                full_path = table_expr.sql(identify=False).upper()

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
                # Join 대상 테이블도 Full Path와 Short Name 추출
                j_full = join.this.sql(identify=False).upper().split(' ')[0]
                j_short = j_full.split('.')[-1]

                on_condition = join.args.get('on').sql() if join.args.get('on') else ""

                joins.append({
                    "full_path": j_full,
                    "short_name": j_short,
                    "on": self._clean_alias(on_condition)
                })

            return {
                "tables": table_info, # 리스트 내 딕셔너리 구조
                "joins": joins
            }
        except Exception as e:
            print(f"❌ 파싱 에러: {e}")
            return None

    def _clean_alias(self, condition):
        if not condition: return ""
        return self.alias_pattern.sub('', condition).upper()