import requests
from requests.auth import HTTPBasicAuth

# OpenSearch에서 Iceberg URN 전체 추출
url = "http://opensearch주소/datasetindex_v2/_search"
auth = HTTPBasicAuth("아이디", "비밀번호")

query = {
    "size": 10000,
    "track_total_hits": True,
    "_source": ["urn"],
    "query": {
        "term": {"platform": "iceberg"}
    }
}

response = requests.post(url, json=query, auth=auth)
hits = response.json()['hits']['hits']

# URN에서 스키마명.테이블명 파싱
# URN 형식: urn:li:dataset:(urn:li:dataPlatform:iceberg,lake_catalog_v2.스키마.테이블,PROD)
tables = []
for hit in hits:
    urn = hit['_source']['urn']
    try:
        parts = urn.split(',')[1]  # lake_catalog_v2.스키마.테이블
        schema = parts.split('.')[1]
        table = parts.split('.')[2]
        tables.append((schema, table))
    except Exception:
        pass

print(f"추출된 테이블 수: {len(tables)}")
print(tables[:5])  # 샘플 5개 확인
