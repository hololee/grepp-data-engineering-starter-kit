--두번째 방법.
SELECT 
	DISTINCT userid,
	FIRST_VALUE(channel) OVER (partition by userid order by ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as start_channel,
	LAST_VALUE(channel) OVER (partition by userid order by ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as end_channel  
FROM raw_data.user_session_channel usc
JOIN raw_data.session_timestamp st ON usc.sessionid = st.sessionid
ORDER BY 1;
