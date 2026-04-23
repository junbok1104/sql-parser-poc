-- 스키마가 없으면 생성 (명시적으로 확인)
CREATE SCHEMA IF NOT EXISTS lake_public;

-- 현재 세션의 작업 경로를 lake_public으로 우선 고정
SET search_path TO lake_public, public;

-- PostgreSQL의 UUID 확장을 활성화 (최초 1회 필요할 수 있음)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp" SCHEMA lake_public;

-- 1. 개별 쿼리 상태 관리 테이블
CREATE TABLE IF NOT EXISTS sql_query_status (
                                                query_id        UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                                                user_id         VARCHAR(50),
                                                session_id      VARCHAR(50),
                                                raw_query       TEXT,
                                                status          VARCHAR(20) DEFAULT 'READ',
                                                parsed_meta     JSONB,                  -- JSONB로 파싱 결과를 통째로 저장
                                                created_at      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                                                updated_at      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 2. 세션 단위 분석 결과 테이블
CREATE TABLE IF NOT EXISTS sql_session_status (
                                                  session_id      VARCHAR(50) PRIMARY KEY,
                                                  user_id         VARCHAR(50),
                                                  status          VARCHAR(20) DEFAULT 'SESSIONIZED',
                                                  ai_summary      JSONB,                  -- AI 요약(description, users 등)을 JSONB로 관리
                                                  start_time      TIMESTAMP WITH TIME ZONE,
                                                  end_time        TIMESTAMP WITH TIME ZONE,
                                                  query_count     INTEGER DEFAULT 0
);