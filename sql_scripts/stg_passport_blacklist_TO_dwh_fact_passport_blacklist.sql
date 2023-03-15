insert into de12.buma_dwh_fact_passport_blacklist
	select
		stg.date,
		stg.passport
	from  de12.buma_stg_passport_blacklist stg
	    left join de12.buma_dwh_fact_passport_blacklist tgt
	    on stg.passport = tgt.passport_num
	where tgt.passport_num is null;