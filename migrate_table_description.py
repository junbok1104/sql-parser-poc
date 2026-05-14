import requests
import oracledb

datahub_url = "http://datahub주소"
token = "발급된토큰값"
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

# 1. DSP에서 테이블 설명 1건 가져오기
conn = oracledb.connect(
    user="아이디",
    password="비밀번호",
    dsn="호스트:포트/서비스명"
)
cursor = conn.cursor()

cursor.execute("""
    SELECT SCHEMA_NM, TABLE_NM, TABLE_DESC
    FROM CATALOGADM.CAT_TAB_MAS
    WHERE TABLE_DESC IS NOT NULL
    AND ROWNUM = 1
""")

row = cursor.fetchone()
schema_nm = row[0].lower()
table_nm = row[1].lower()
table_desc = row[2]

print(f"테이블: {schema_nm}.{table_nm}")
print(f"설명: {table_desc}")

# 2. DataHub URN 생성
urn = f"urn:li:dataset:(urn:li:dataPlatform:iceberg,lake_catalog_v2.{schema_nm}.{table_nm},PROD)"
print(f"URN: {urn}")

# 3. DataHub API로 설명 업데이트
payload = {
    "proposal": {
        "entityType": "dataset",
        "entityUrn": urn,
        "aspectName": "editableDatasetProperties",
        "aspect": {
            "value": f'{{"description": "{table_desc}"}}',
            "contentType": "application/json"
        }
    }
}

response = requests.post(
    f"{datahub_url}/api/v2/aspects",
    headers=headers,
    json=payload
)

print(f"응답코드: {response.status_code}")
print(response.json())

cursor.close()
conn.close()
