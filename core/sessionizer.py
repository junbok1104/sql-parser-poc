from datetime import datetime, timedelta

class Sessionizer:
    def __init__(self, timeout_minutes=5):
        # WBS SQL 1.3: 5분 타임아웃 기준 설정
        self.timeout = timedelta(minutes=timeout_minutes)

    def group_by_session(self, logs):
        if not logs: return []

        # 1. 유저별, 시간순 정렬
        sorted_logs = sorted(logs, key=lambda x: (x['user_id'], x['timestamp']))

        sessionized_data = []
        current_session_id = 1

        if sorted_logs:
            # 첫 번째 로그 초기화
            last_log = sorted_logs[0]
            last_log['session_id'] = f"SESS_{current_session_id:04d}"
            sessionized_data.append(last_log)

            for i in range(1, len(sorted_logs)):
                current_log = sorted_logs[i]

                # 동일 유저이고, 이전 쿼리와의 시간 차이가 5분 이내인가?
                time_diff = current_log['timestamp'] - last_log['timestamp']

                if current_log['user_id'] == last_log['user_id'] and time_diff <= self.timeout:
                    # 동일 세션 유지
                    current_log['session_id'] = last_log['session_id']
                else:
                    # 새로운 세션 시작
                    current_session_id += 1
                    current_log['session_id'] = f"SESS_{current_session_id:04d}"

                sessionized_data.append(current_log)
                last_log = current_log

        return sessionized_data