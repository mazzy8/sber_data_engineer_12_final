insert into de12.buma_dwh_dim_accounts
	select 
		account,
		valid_to,
		client
	from de12.buma_stg_accounts;