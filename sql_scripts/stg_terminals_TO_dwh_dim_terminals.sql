insert into de12.buma_dwh_dim_terminals
	select
		terminal_id,
		terminal_type,
		terminal_city,
		terminal_address
	from de12.buma_stg_terminals;