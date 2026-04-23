import os
from database.connection import engine
from sqlalchemy import text

def init_tables():
    # 1. 현재 파일(init_db.py)의 위치를 기준으로 schema.sql 경로 계산
    current_dir = os.path.dirname(os.path.abspath(__file__))
    schema_path = os.path.join(current_dir, "schema.sql")

    with engine.connect() as conn:
        # 2. 계산된 절대 경로로 파일 열기
        with open(schema_path, "r", encoding="utf-8") as f:
            sql_queries = f.read().split(';')

            for query in sql_queries:
                if query.strip():
                    conn.execute(text(query))
        conn.commit()
    print("✅ PostgreSQL 테이블 및 UUID 확장 생성 완료 (SQL 1.4)")

if __name__ == "__main__":
    init_tables()