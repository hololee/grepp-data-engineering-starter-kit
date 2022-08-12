SELECT userid, SUM(COALESCE(amount, 0)) as grossrevenue
FROM 
    raw_data.session_timestamp st
JOIN 
    raw_data.user_session_channel usc 
ON 
    st.sessionid = usc.sessionid
LEFT JOIN 
    raw_data.session_transaction strn
ON 
    st.sessionid = strn.sessionid
GROUP BY 1
order by 2 DESC
LIMIT 10;
