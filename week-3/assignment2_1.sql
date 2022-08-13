--첫번째 방법.
WITH 
start_ts AS(
	SELECT userid, ts, channel as start_channel, ROW_NUMBER() OVER (partition by userid order by ts ASC) seq 
	FROM raw_data.user_session_channel usc
	JOIN raw_data.session_timestamp st ON usc.sessionid = st.sessionid
),
end_ts AS(
	SELECT userid, ts, channel as end_channel, ROW_NUMBER() OVER (partition by userid order by ts DESC) seq 
	FROM raw_data.user_session_channel usc
	JOIN raw_data.session_timestamp st ON usc.sessionid = st.sessionid
)

SELECT start_ts.userid, start_channel, end_channel
FROM start_ts
JOIN end_ts ON start_ts.userid = end_ts.userid
WHERE start_ts.seq=1
AND end_ts.seq=1
ORDER BY 1;
