insert into de12.buma_dwh_fact_transactions
	select 
		transaction_id,
		transaction_date,
		card_num,
		oper_type,
		amount,
		oper_result,
		terminal
	from de12.buma_stg_transactions;