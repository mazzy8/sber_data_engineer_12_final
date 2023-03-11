insert into de12.buma_dwh_fact_transactions
	select 
		trans_id,
		trans_date,
		card_num,
		oper_type,
		replace(amount, ',','.')::decimal,
		oper_result,
		terminal
	from de12.buma_stg_transactions;