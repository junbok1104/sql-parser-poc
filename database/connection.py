import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# 도커 환경 변수에서 정보를 읽어옵니다. (기본값 설정 가능)
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432") # 사용하시는 DB 포트로 수정
DB_USER = os.getenv("DB_USER", "admin")
DB_PASS = os.getenv("DB_PASS", "Junbok1234!!")
DB_NAME = os.getenv("DB_NAME", "lake_db")

# DB 종류에 맞는 드라이버를 지정하세요.
# 예: "postgresql+psycopg2://...", "mysql+pymysql://..."
DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DB_URL, echo=False)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()