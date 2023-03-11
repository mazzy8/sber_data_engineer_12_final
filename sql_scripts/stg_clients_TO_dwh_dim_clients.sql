insert into de12.buma_dwh_dim_clients
	select 
		client_id,
		last_name,
		first_name,
		patronymic,
		date_of_birth,
		passport_num,
		passport_valid_to,
		phone
	from de12.buma_stg_clients;