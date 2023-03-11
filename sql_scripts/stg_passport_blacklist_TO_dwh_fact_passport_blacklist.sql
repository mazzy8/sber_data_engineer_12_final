insert into de12.buma_fact_passport_blacklist
	select 
		date,
		passport
	from de12.buma_stg_passport_blacklist;