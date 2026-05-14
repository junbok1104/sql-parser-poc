# DSP-DataHub 인터페이스 정의서 현황조사

## 1. 개요

### 목적
- DSP Deprecation 예정에 따라 DSP에서 관리하는 테이블/컬럼 설명과 Tag 정보를 Apache DataHub로 이관
- DataHub Iceberg Dataset 기준으로 메타데이터 누락/미정제 상태를 체계적으로 정리
- 마이그레이션 작업 범위와 우선순위 결정의 근거로 활용

### 분석 대상
- DataHub 등록 Iceberg Dataset

### 접근 수단
- DataHub UI
- OpenSearch
- DSP Oracle DB 직접 접속 (Airgap 환경)

---

## 2. 시스템 구조 이해

```
E-Hub(DSP) → Lake1.0 → Lake2.0 → Apache DataHub
```

| 시스템 | 역할 | 비고 |
|---|---|---|
| DSP(E-Hub) | 운영 원천 데이터 | Oracle, Lake1.0 관리 |
| Lake1.0 | 구버전 데이터레이크 | Hive/임팔라 기반 |
| Lake2.0 | 신버전 데이터레이크 | Iceberg/Polaris 기반 |
| Apache DataHub | 메타데이터 카탈로그 | 이관 대상 |

---

## 3. DSP Catalog DB 조사

### 3-1. 테이블 스키마 확인

**접속 정보**
- 스키마 : CATALOGADM
- 전체 테이블 수 : 134개

**주요 테이블**

| 테이블 | 역할 |
|---|---|
| CAT_TAB_MAS | 테이블 메타정보 관리 |
| CAT_COL_INF | 컬럼 메타정보 관리 |

**CAT_TAB_MAS 주요 컬럼**

| 컬럼명 | 설명 |
|---|---|
| SCHEMA_NM | 스키마명 |
| TABLE_NM | 테이블명 |
| TABLE_DESC | 테이블 설명 |
| TAG_LVAL | Tag 값 (JSON 배열 형태) |
| SYS_ID | 시스템 구분 |

**TAG_LVAL 구조**
```json
[{"tagIdx":"FAB DATA MART", "tagVal":"FAB DATA MART"},
 {"tagIdx":"MART", "tagVal":"MART"}]
```

---

### 3-2. dataset_id 생성규칙 확인

**조사 결과**
- DSP에서 URN 형태의 식별자 컬럼 없음
- Oracle 타입만 컨테이너 URN 존재 (`urn:li:container:fe122~`)
- Hive/Iceberg 타입 URN 없음

**DSP가 관리하는 시스템**
```
DSP
 ├─ DataHub (E-Hub) - Oracle
 └─ DataLake (Lake1.0) - Hive/임팔라
    └─ Lake2.0 Iceberg는 DSP 관할 밖
```

**대안 매칭 키 확정**
```
DSP SCHEMA_NM + TABLE_NM
        ↕
DataHub Iceberg URN 파싱
└─ lake_catalog_v2.스키마명.테이블명

DataHub Iceberg URN 형식
urn:li:dataset:(urn:li:dataPlatform:iceberg,lake_catalog_v2.스키마명.테이블명,PROD)
```

---

### 3-3. Description 데이터 품질 측정

**조회 쿼리**
```sql
SELECT
  COUNT(*) AS 전체건수,
  SUM(CASE WHEN TABLE_DESC IS NULL THEN 1 ELSE 0 END) AS NULL건수,
  ROUND(
    SUM(CASE WHEN TABLE_DESC IS NULL THEN 1 ELSE 0 END)
    / COUNT(*) * 100, 2
  ) AS NULL비율,
  SUM(CASE WHEN LENGTH(TABLE_DESC) < 5 THEN 1 ELSE 0 END) AS 짧은값건수
FROM CATALOGADM.CAT_TAB_MAS;
```

**품질 기준 결정**
- NULL/공백 → 공백으로 이관
- 최소 길이 기준 없음 (있는 값 그대로 이관)

---

### 3-4. Tag 전수 목록 추출

**조회 쿼리**
```sql
SELECT
  jt.tagVal,
  COUNT(*) AS 빈도
FROM CATALOGADM.CAT_TAB_MAS t,
JSON_TABLE(t.TAG_LVAL, '$[*]'
  COLUMNS (
    tagVal VARCHAR2(100) PATH '$.tagVal'
  )
) jt
WHERE t.TAG_LVAL IS NOT NULL
GROUP BY jt.tagVal
ORDER BY 빈도 DESC;
```

**산출물**
- 한글 Tag 목록 + 빈도 엑셀 추출 완료
- 영문 코드 변환 규칙 미확정 (추후 공유 예정)

---

### 3-5. DSP ↔ DataHub 매칭키 검증

**건수 현황**

| 시스템 | 건수 | 비고 |
|---|---|---|
| DSP DataLake 적재 | 15,827건 | Lake1.0 임팔라 API 실시간 조회 |
| DataHub Lake1.0 (Hive) | 14,595건 | lake_catalog 기준 |
| DataHub Lake2.0 (Iceberg) | 9,272건 | lake_catalog_v2 기준 |

**건수 차이 원인**
```
DSP 15,827건 vs DataHub Lake1.0 14,595건
 └─ DSP → 실시간 API 조회
 └─ DataHub → Ingestion 시점 스냅샷
 └─ 완전 일치하지 않는 게 자연스러운 상태

DataHub Lake1.0 14,595건 vs Lake2.0 9,272건
 └─ Lake1.0 전체가 Lake2.0으로 이관 안됨
 └─ 이관된 것만 Lake2.0 Iceberg에 존재
```

**매칭 방향 확정**
```
DataHub Iceberg 9,272건 기준 역방향 매칭
 └─ DataHub Iceberg 테이블명
     └─▶ DSP에서 동일 테이블명 조회
          └─ 있으면 → 메타정보 이관
          └─ 없으면 → 이관 대상 제외 (공백 유지)
```

**컬럼 불일치 케이스 발견**
```
DSP 컬럼 수 > DataHub Iceberg 컬럼 수
예) 특정 테이블 DSP 19개 / DataHub 18개
 └─ MT 컬럼이 Iceberg에 없음
 └─ DataHub Iceberg 컬럼 기준으로만 이관
```

**미확정 사항**
- 정확한 매칭률 수치 측정 필요 (목표 95% 이상)
- 매칭 실패 처리 방침 미확정

---

## 4. OpenSearch / DataHub UI 조사

### DataHub Aspect vs OpenSearch 필드명

| DataHub Aspect | OpenSearch 필드 | 생성 방식 |
|---|---|---|
| datasetProperties | description | Ingestion 자동 |
| editableDatasetProperties | editedDescription | 수동/API |
| schemaMetadata | fieldDescriptions | Ingestion 자동 |
| editableSchemaMetadata | editedFieldDescriptions | 수동/API |

---

### 4-1. editableDatasetProperties 채움비율

**조회 쿼리**
```json
GET datasetindex_v2/_search
{
  "size": 0,
  "track_total_hits": true,
  "query": {
    "bool": {
      "must": [
        {"term": {"platform": "iceberg"}},
        {"exists": {"field": "editedDescription"}}
      ]
    }
  }
}
```

**결과**

| 항목 | 건수 |
|---|---|
| 전체 Iceberg | 9,272건 |
| 설명 있는 것 | 1건 |
| 채움비율 | 0.01% |

**결론** : G-01 확인 완료 - 거의 미입력 상태

---

### 4-2. editableSchemaMetadata 채움비율

**조회 쿼리**
```json
GET datasetindex_v2/_search
{
  "size": 0,
  "track_total_hits": true,
  "query": {
    "bool": {
      "must": [
        {"term": {"platform": "iceberg"}},
        {"exists": {"field": "editedFieldDescriptions"}}
      ]
    }
  }
}
```

**결과**

| 항목 | 건수 |
|---|---|
| 전체 Iceberg | 9,272건 |
| 컬럼설명 있는 것 | 0건 |
| 채움비율 | 0% |

**참고**
- fieldDescriptions (자동 수집) : 338건 존재
- editedFieldDescriptions (수동 입력) : 0건

**결론** : G-02 확인 완료 - 전혀 없는 상태

---

### 4-3. Tag URN 목록 vs Tag Entity

**Tag Entity 현황**
```
DataHub Tags 메뉴 확인
 └─ 활성 Tag Entity : 1개 (area)
 └─ tagindex_v2 전체 건수 : 45,036건
    (삭제/비활성 포함)
```

**Tag URN 있는 Dataset 건수**
```json
GET datasetindex_v2/_search
{
  "size": 0,
  "query": {
    "bool": {
      "must": [
        {"term": {"platform": "iceberg"}},
        {"exists": {"field": "tags"}}
      ]
    }
  }
}
```

**결과**

| 항목 | 건수 |
|---|---|
| Tag URN 있는 Dataset | 219건 |
| 활성 Tag Entity | 1개 (area) |
| Orphan Tag | area 제외 나머지 전부 |

**Orphan Tag란**
```
Tag URN은 있는데 Tag Entity가 없는 상태
 └─ UI 필터 동작 안함
 └─ 클릭해도 페이지 이동 안함
```

**결론** : G-03 확인 완료 - Tag Entity 선행 생성 필요

---

### 4-4. CustomProperty Key 종류 및 빈도

**조회 쿼리**
```json
GET datasetindex_v2/_search
{
  "size": 0,
  "query": {
    "term": {"platform": "iceberg"}
  },
  "aggs": {
    "custom_props": {
      "terms": {
        "field": "customProperties",
        "size": 200
      }
    }
  }
}
```

**주요 발견 Key 유형**

| Key 예시 | 유형 | 비고 |
|---|---|---|
| format-version=2 | Iceberg 기술 설정값 | 정제 대상 |
| write.metadata.delete-after-commit.enabled=true | Iceberg 기술 설정값 | 정제 대상 |
| commit.retry.max-wait-ms=30000 | Iceberg 기술 설정값 | 정제 대상 |
| table_type=ICEBERG | 타입 구분값 | 19,353건 |

**결론** : G-04 확인 완료 - Iceberg 기술 설정값이 대부분, 표준 Key 정의 필요

---

### 4-5. Domain Aspect 설정현황

**조회 쿼리**
```json
GET datasetindex_v2/_search
{
  "size": 0,
  "track_total_hits": true,
  "query": {
    "bool": {
      "must": [
        {"term": {"platform": "iceberg"}},
        {"exists": {"field": "domains"}}
      ]
    }
  }
}
```

**결과**

| 항목 | 건수 |
|---|---|
| 전체 Iceberg | 9,272건 |
| Domain 설정된 것 | 9,272건 |
| 미설정 비율 | 0% |

**문제점**
```
Domain 값 = urn:li:domain:lake_catalog_v2
 └─ 카탈로그명으로 일괄 설정
 └─ 업무 언어 기반 Domain 아님
 └─ 실질적으로 G-05 미해결 상태
```

**결론** : G-05 확인 완료 - 업무 영역 기반 Domain 재설계 필요

---

### 4-6. 한글 Tag 목록

**DataHub Iceberg Tag 현황**
```
tags 필드 값 형식
 └─ urn:li:tag:Lot Grouping (영문)
 └─ urn:li:tag:한글값 (한글)
 └─ 한글/영문 혼재 상태
```

**DSP Tag 목록**
- JSON 파싱 쿼리로 추출 완료
- 엑셀 저장 완료
- 영문 코드 변환 규칙 미확정 (추후 공유 예정)

**결론** : 한글/영문 혼재 상태, 변환 규칙 정의 후 정제 필요

---

## 5. GAP 현황 요약

| GAP | 내용 | 현황 | 해결 방향 |
|---|---|---|---|
| G-01 | 테이블 Description 누락 | 0.01% 채움 | DSP TABLE_DESC → editableDatasetProperties 이관 |
| G-02 | 컬럼 Description 누락 | 0% 채움 | DSP CAT_COL_INF → editableSchemaMetadata 이관 |
| G-03 | Tag 필터 미동작 | Orphan Tag 다수 | Tag Entity 선행 생성 + globalTags 재적재 |
| G-04 | CustomProperty 규칙 없음 | Iceberg 기술값 혼재 | 표준 Key 정의 + 비표준 정제 |
| G-05 | Domain 미설정 | 카탈로그명으로 일괄 설정 | 9개 업무영역 기반 Domain 재설계 |
| G-06 | 검색품질 저하 | G-01~03 미해소 | G-01~03 해소시 자동 개선 |

---

## 6. 이관 기준 확정

| 항목 | 기준 |
|---|---|
| 이관 범위 | DataHub Iceberg 타입 전체 (9,272건) |
| Description 없는 경우 | 이관 대상 제외 (공백 유지) |
| 컬럼 불일치 | DataHub Iceberg 컬럼 기준 매칭 |
| Tag 변환 규칙 | 추후 확정 |
| 개발 방식 | 파이썬 스크립트 |

---

## 7. 미확정 사항 (킥오프 확인 필요)

```
① Tag 한글 → 영문 변환 규칙
② DataHub API 접근 권한
③ 매칭 실패 처리 방침
④ Polaris 접근 방법
⑤ 이관 마감 일정
⑥ 정확한 매칭률 수치 측정 (목표 95% 이상)
```
