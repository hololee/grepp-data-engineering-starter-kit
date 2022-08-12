-- DROP TABLE IF EXISTS zayden.monthly_user_paid_summary;
CREATE TABLE zayden.monthly_user_paid_summary AS
	SELECT 
		TO_CHAR(st.ts, 'YYYY-mm') as "year-month",
		channel,
		COUNT(DISTINCT usc.userid) as uniqueUsers,
		COUNT(CASE strn.amount WHEN strn.amount > 0 THEN 1 WHEN strn.amount = 0 THEN NULL END) as paidUsers,
		CONVERT(DECIMAL(20,6),CONVERT(FLOAT, paidUsers) / NULLIF(uniqueUsers, 0)) as conversionRate,
		SUM(COALESCE(amount, 0)) as grossrevenue,
		SUM(COALESCE(CASE strn.refunded WHEN TRUE THEN 0 WHEN FALSE THEN strn.amount END, 0)) as netrevenue
	FROM raw_data.session_timestamp st
	JOIN 
		raw_data.user_session_channel usc 
	ON 
		st.sessionid = usc.sessionid
	LEFT JOIN 
		raw_data.session_transaction strn
	ON 
		st.sessionid = strn.sessionid
	GROUP BY 1, 2
	ORDER BY 1;	
-- select * from zayden.monthly_user_paid_summary;
