# 월별마다 액티브한 사용자들의 수
%%sql

    SELECT TO_CHAR(DATE(ts), 'YYYY-MM') as DATE, COUNT(DISTINCT userid) AS USER_COUNT
      FROM raw_data.session_timestamp a
      JOIN raw_data.user_session_channel b
        ON a.sessionID = b.sessionID
  GROUP BY 1
  ORDER BY 1


# 유입 채널별 증가 추이
# 모든 채널에서 증감 양상이 비슷하게 나타나는 중
%%sql

    SELECT TO_CHAR(DATE(ts), 'YYYY-MM') as DATE, channel, COUNT(DISTINCT userid) AS USER_COUNT
      FROM raw_data.session_timestamp a
      JOIN raw_data.user_session_channel b
        ON a.sessionID = b.sessionID
  GROUP BY 1, 2
  ORDER BY 2, 1