<img width="500" height="500" alt="image" src="https://github.com/user-attachments/assets/001702ab-1a36-4083-bc55-48d388e42b8e" />


# 🚀 SQL Parser POC (Lineage & Metadata Extractor)

`sqlglot` 라이브러리를 활용하여 복잡한 SQL 로그로부터 테이블 간의 연관 관계(Lineage) 및 핵심 메타데이터를 추출하는 분석 엔진입니다.

## 1. 🎯 프로젝트 배경 및 목적
사내의 방대한 SQL 로그를 분석하여 데이터 흐름을 시각화하고, 특정 테이블 변경 시 영향도를 사전에 파악하기 위한 POC(Proof of Concept) 프로젝트입니다.
- **Data Lineage**: 테이블 간의 참조 관계를 그래프로 시각화
- **Metadata Insight**: 사용자가 자주 조회하는 지표(`interests`) 및 필터 조건(`hotFilters`) 자동 추출
- **Impact Analysis**: 데이터 모델 변경 시 영향 범위 파악 기초 자료 확보

## 2. 🛠 Tech Stack
- **Language**: Python 3.10+
- **SQL Parser**: [sqlglot](https://github.com/tobymao/sqlglot)
- **Visualization**: Pyvis (Interactive Network Graph)
- **Dependency**: Poetry

## 3. ✨ 주요 기능 (Key Features)
- **복합 SQL 파싱**: CTE(WITH), UNION ALL, Subquery, Window Function 등 실전형 SQL 해석
- **지표 추출**:
    - `interests`: SELECT 절 컬럼 기반 관심 지표 식별
    - `hotFilters`: WHERE 절 기반 빈번 사용 조건 및 바인드 변수 추출
    - `aggregations`: GROUP BY / ORDER BY 컬럼 추출
- **시각화**: 테이블 간 JOIN 횟수를 가중치로 반영한 네트워크 그래프 생성

---

## 🧪 4. 분석 결과 샘플 (Test Evidence)

### Case A: 복잡한 분석 쿼리 (CTE & Window Function)
<details>
<summary>🔍 쿼리 원문 및 JSON 결과 보기</summary>

**Input SQL**
```sql
WITH RECENT_LOTS AS (
    SELECT LH.LOT_ID, LH.PROD_CODE, LH.STATION_ID, LH.CREATE_DT
    FROM LAKE_CATALOG.MDM.LOT_HIST AS LH
    WHERE LH.CREATE_DT >= :start_date AND LH.STATUS IN ('COMPLETED', 'SHIPPED')
),
STATS_DATA AS (
    SELECT 
        RD.PROD_CODE,
        AVG(MD.VALUE) OVER (PARTITION BY RD.PROD_CODE) as AVG_VALUE,
        COUNT(MD.MEASURE_ID) as MEASURE_CNT
    FROM RECENT_LOTS RD
    LEFT JOIN LAKE_CATALOG.MDM.MEASUREMENT_DATA AS MD ON RD.LOT_ID = MD.LOT_ID
    GROUP BY RD.PROD_CODE, RD.STATION_ID, MD.VALUE
)
SELECT A.PROD_CODE, B.PROD_NAME, A.AVG_VALUE,
       (SELECT COUNT(*) FROM LAKE_CATALOG.MDM.ALARM_HIST WHERE PROD_CODE = A.PROD_CODE) as ALARM_COUNT
FROM STATS_DATA A
JOIN LAKE_CATALOG.MDM.MDM_LFB_PROD_M AS B ON A.PROD_CODE = B.PROD_CODE
WHERE A.MEASURE_CNT > 10
ORDER BY A.AVG_VALUE DESC;
```

***Parsed Result (JSON)***
```sql
{
  "tables": [
    {"full_path": "LAKE_CATALOG.MDM.LOT_HIST", "short_name": "LOT_HIST"},
    {"full_path": "LAKE_CATALOG.MDM.MEASUREMENT_DATA", "short_name": "MEASUREMENT_DATA"},
    {"full_path": "LAKE_CATALOG.MDM.ALARM_HIST", "short_name": "ALARM_HIST"},
    {"full_path": "LAKE_CATALOG.MDM.MDM_LFB_PROD_M", "short_name": "MDM_LFB_PROD_M"},
    {"full_path": "RECENT_LOTS", "short_name": "RECENT_LOTS"},
    {"full_path": "STATS_DATA", "short_name": "STATS_DATA"}
  ],
  "interests": ["AVG_VALUE", "PROD_NAME", "ALARM_COUNT"],
  "hotFilters": ["CREATE_DT >= :start_date", "STATUS IN ('COMPLETED', 'SHIPPED')", "MEASURE_CNT > 10"],
  "aggregations": ["PROD_CODE", "STATION_ID"]
}
```
</details>


## 🚧 5. 한계점 및 향후 과제 (Core Issues)
실전 테스트를 통해 도출된 개선 필요 사항입니다.
1. 가상 테이블(CTE) 노드화: WITH절 임시 테이블이 물리 노드로 표시되는 현상 (필터링 로직 필요)
2. 별칭(Alias) 파편화: LH., RD. 등 별칭에 따라 동일 컬럼이 중복 추출되는 이슈 (정규화 필요)
3. UNION 컨텍스트 소실: UNION 결합 시 각 블록별 필터 조건의 매핑 관계 모호 (블록 단위 파싱 필요)
4. 사내 UDF 해석: 특화된 사용자 정의 함수의 함수명 식별 및 가중치 부여 기능 미흡

## 6. 실행 방법
***환경 구축***
```sql
poetry install
```

***분석 실행***
```sql
poetry run python main.py
```





