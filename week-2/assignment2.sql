SELECT TO_CHAR(st.ts, 'YYYY-mm') date, COUNT(usc.userID) nums
FROM raw_data.session_timestamp st
JOIN raw_data.user_session_channel usc ON st.sessionID = usc.sessionID
GROUP BY 1 -- st.ts가 집계함수나 group by 절에 들어가야함. count가 그룹화를 시키지만 date는 그룹 조건이 없기때문에 오류 발생.
ORDER BY 1; -- group by, order by 에 따라오는 숫자는 column number를 의미함.
