from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os
from dotenv import load_dotenv

load_dotenv()

# 개별 환경변수로 URL 구성
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME') 
DB_HOST = os.getenv('DB_HOST')  
DB_PORT = os.getenv('DB_PORT')  

if not all([DB_USER, DB_PASSWORD]):
    raise ValueError("DB_URL 또는 DB_USER, DB_PASSWORD 환경변수가 필요합니다.")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


# 디버깅을 위한 URL 출력 (패스워드 마스킹)
masked_url = DATABASE_URL
if ':password@' in masked_url:
    masked_url = masked_url.replace(':password@', ':***@')
else:
    # 다른 패스워드 패턴도 마스킹
    import re
    masked_url = re.sub(r':([^@]+)@', ':***@', masked_url)
print(f"데이터베이스 연결 URL: {masked_url}")

# SQLAlchemy 엔진 생성
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_recycle=300,
    echo=False  # SQL 로그 출력 (개발시에만)
)

# 세션 로컬 생성
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base 클래스 생성
Base = declarative_base()

# 데이터베이스 세션 의존성
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close() 