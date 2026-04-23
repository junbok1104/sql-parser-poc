import sqlglot
from sqlglot import exp
import re

class SQLParser:
    def __init__(self):
        # 정규화 로직: 별칭(Alias) 제거용 정규식
        self.alias_pattern = re.compile(r'\b\w+\.')

    def parse_query(self, sql):
        """
        SQL을 분석하여 테이블, 조인 조건 등을 추출합니다. (SQL 1.2 대응)
        """
        try:
            expression = sqlglot.parse_one(sql)

            # 1. 테이블 추출 및 정규화 (split('.')[-1])
            tables = [t.this.sql().split('.')[-1] for t in expression.find_all(exp.Table)]

            # 2. 조인 조건 추출
            joins = []
            for join in expression.find_all(exp.Join):
                table_raw = join.this.sql().split(' ')[0]
                table_name = table_raw.split('.')[-1]
                on_condition = join.args.get('on').sql() if join.args.get('on') else ""

                joins.append({
                    "table": table_name,
                    "on": self._clean_alias(on_condition)
                })

            return {
                "tables": list(set(tables)), # 중복 제거
                "joins": joins
            }
        except Exception as e:
            print(f"❌ 파싱 에러: {e}")
            return None

    def _clean_alias(self, condition):
        # 별칭 제거 로직 (lh.ID -> ID)
        if not condition: return ""
        return self.alias_pattern.sub('', condition).upper()