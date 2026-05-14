import requests
import json

datahub_url = "http://datahub주소"
token = "토큰값"

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

urn = "urn:li:dataset:(urn:li:dataPlatform:iceberg,lake_catalog_v2.apc.아까테이블명,PROD)"

payload = {
    "proposal": {
        "entityType": "dataset",
        "entityUrn": urn,
        "aspectName": "editableDatasetProperties",
        "aspect": {
            "value": json.dumps({"description": "테스트설명"}),
            "contentType": "application/json"
        },
        "changeType": "UPSERT"
    }
}

response = requests.post(
    f"{datahub_url}/api/gms/aspects?action=ingestProposal",
    headers=headers,
    json=payload
)

print(response.status_code)
print(response.text)
