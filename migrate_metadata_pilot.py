import requests
import oracledb
import json

# ==================
# 설정값
# ==================
DATAHUB_URL = "http://개발datahub주소"
USERNAME = "아이디"
PASSWORD = "비밀번호"

DB_USER = "아이디"
DB_PASSWORD = "비밀번호"
DB_DSN = "호스트:포트/서비스명"

# ==================
# 연결
# ==================

# DataHub 로그인
session = requests.Session()
login_response = session.post(
    f"{DATAHUB_URL}/logIn",
    json={"username": USERNAME, "password": PASSWORD}
)
print(f"DataHub 로그인: {login_response.status_code}")

# Oracle 연결
conn = oracledb.connect(
    user=DB_USER,
    password=DB_PASSWORD,
    dsn=DB_DSN
)
cursor = conn.cursor()

# ==================
# DataHub API 함수
# ==================

def update_table_description(urn, description):
    """테이블 설명 업데이트"""
    payload = {
        "proposal": {
            "entityType": "dataset",
            "entityUrn": urn,
            "aspectName": "editableDatasetProperties",
            "aspect": {
                "value": json.dumps({
                    "description": description if description else ""
                }),
                "contentType": "application/json"
            },
            "changeType": "UPSERT"
        }
    }
    response = session.post(
        f"{DATAHUB_URL}/api/gms/aspects?action=ingestProposal",
        json=payload
    )
    return response.status_code

def update_column_descriptions(urn, fields):
    """컬럼 설명 업데이트"""
    payload = {
        "proposal": {
            "entityType": "dataset",
            "entityUrn": urn,
            "aspectName": "editableSchemaMetadata",
            "aspect": {
                "value": json.dumps({
                    "editableSchemaFieldInfo": fields
                }),
                "contentType": "application/json"
            },
            "changeType": "UPSERT"
        }
    }
    response = session.post(
        f"{DATAHUB_URL}/api/gms/aspects?action=ingestProposal",
        json=payload
    )
    return response.status_code

# ==================
# 이관 실행
# ==================

# DSP에서 테이블 목록 가져오기
cursor.execute("""
    SELECT
        SCHEMA_NM,
        TABLE_NM,
        TABLE_DESC
    FROM CATALOGADM.CAT_TAB_MAS
    WHERE TABLE_DESC IS NOT NULL
    AND ROWNUM <= 5
""")

tables = cursor.fetchall()
print(f"이관 대상 테이블: {len(tables)}건")

success = 0
fail = 0

for table in tables:
    schema_nm = table[0].lower()
    table_nm = table[1].lower()
    table_desc = table[2] if table[2] else ""

    # URN 생성
    urn = f"urn:li:dataset:(urn:li:dataPlatform:iceberg,lake_catalog_v2.{schema_nm}.{table_nm},PROD)"

    print(f"\n처리중: {schema_nm}.{table_nm}")

    # 1. 테이블 설명 업데이트
    status = update_table_description(urn, table_desc)
    print(f"  테이블 설명: {status}")

    # 2. 컬럼 설명 가져오기
    cursor.execute("""
        SELECT
            c.COLUMN_NM,
            c.COLUMN_DESC
        FROM CATALOGADM.CAT_COL_INF c
        JOIN CATALOGADM.CAT_TAB_MAS t
            ON c.TABLE_ID = t.TABLE_ID
        WHERE t.SCHEMA_NM = :1
        AND t.TABLE_NM = :2
    """, [schema_nm.upper(), table_nm.upper()])

    columns = cursor.fetchall()

    # 3. 컬럼 설명 구성
    fields = []
    for col in columns:
        fields.append({
            "fieldPath": col[0].lower(),
            "description": col[1] if col[1] else ""
        })

    # 4. 컬럼 설명 업데이트
    if fields:
        status = update_column_descriptions(urn, fields)
        print(f"  컬럼 설명: {status} ({len(fields)}개 컬럼)")

    if status == 200:
        success += 1
    else:
        fail += 1

# ==================
# 결과 출력
# ==================
print(f"\n{'='*30}")
print(f"이관 완료")
print(f"성공: {success}건")
print(f"실패: {fail}건")

cursor.close()
conn.close()
