import requests
from requests.auth import HTTPBasicAuth

url = "http://opensearch주소/datasetindex_v2/_search"
auth = HTTPBasicAuth("아이디", "비밀번호")

def search(query):
    response = requests.post(url, json=query, auth=auth)
    return response.json()

# 1번 - editableDatasetProperties 채움비율
result1 = search({
    "size": 0,
    "track_total_hits": True,
    "query": {
        "bool": {
            "must": [
                {"term": {"platform": "iceberg"}},
                {"exists": {"field": "editedDescription"}}
            ]
        }
    }
})
edited_desc = result1['hits']['total']['value']

# 2번 - editableSchemaMetadata 채움비율
result2 = search({
    "size": 0,
    "track_total_hits": True,
    "query": {
        "bool": {
            "must": [
                {"term": {"platform": "iceberg"}},
                {"exists": {"field": "editedFieldDescriptions"}}
            ]
        }
    }
})
edited_field = result2['hits']['total']['value']

# 3번 - Tag URN 있는 Dataset 건수
result3 = search({
    "size": 0,
    "track_total_hits": True,
    "query": {
        "bool": {
            "must": [
                {"term": {"platform": "iceberg"}},
                {"exists": {"field": "tags"}}
            ]
        }
    }
})
tag_count = result3['hits']['total']['value']

# 4번 - CustomProperty Key 종류/빈도
result4 = search({
    "size": 0,
    "query": {"term": {"platform": "iceberg"}},
    "aggs": {
        "custom_props": {
            "terms": {
                "field": "customProperties",
                "size": 200
            }
        }
    }
})
custom_props = result4['aggregations']['custom_props']['buckets']

# 5번 - Domain 설정현황
result5 = search({
    "size": 0,
    "track_total_hits": True,
    "query": {
        "bool": {
            "must": [
                {"term": {"platform": "iceberg"}},
                {"exists": {"field": "domains"}}
            ]
        }
    }
})
domain_count = result5['hits']['total']['value']

# 전체 Iceberg 건수
total = search({
    "size": 0,
    "track_total_hits": True,
    "query": {"term": {"platform": "iceberg"}}
})
total_count = total['hits']['total']['value']

# 결과 출력
print("=" * 50)
print("DataHub Iceberg 메타데이터 현황 조사")
print("=" * 50)
print(f"\n전체 Iceberg 건수 : {total_count}건")
print(f"\n[G-01] editableDatasetProperties 채움비율")
print(f"  설명 있는 것 : {edited_desc}건")
print(f"  채움비율 : {edited_desc/total_count*100:.2f}%")
print(f"\n[G-02] editableSchemaMetadata 채움비율")
print(f"  컬럼설명 있는 것 : {edited_field}건")
print(f"  채움비율 : {edited_field/total_count*100:.2f}%")
print(f"\n[G-03] Tag URN 있는 Dataset")
print(f"  Tag URN 있는 것 : {tag_count}건")
print(f"  Tag Entity : area 1개 (Orphan Tag 다수)")
print(f"\n[G-04] CustomProperty Key 종류/빈도 (상위 10개)")
for prop in custom_props[:10]:
    print(f"  {prop['key']} : {prop['doc_count']}건")
print(f"\n[G-05] Domain 설정현황")
print(f"  Domain 설정된 것 : {domain_count}건")
print(f"  미설정 비율 : {(total_count-domain_count)/total_count*100:.2f}%")
print(f"  ※ lake_catalog_v2 카탈로그명으로 일괄 설정")
