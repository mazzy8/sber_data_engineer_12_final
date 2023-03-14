insert into de12.buma_dwh_dim_accounts (account_num, valid_to, client, start_dt, end_dt, deleted_flg)
select 
	stg.account , 
	stg.valid_to , 
	stg.client ,
	stg.create_dt, 
	to_date('9999-12-31','YYYY-MM-DD'),
	'N'
from de12.buma_stg_accounts stg
left join de12.buma_dwh_dim_accounts tgt
on stg.account  = tgt.account_num
where tgt.account_num is null;
update de12.buma_dwh_dim_accounts
set 
	end_dt = tmp.update_dt - interval '1 day'
from (
	select 
		stg.account, 
		stg.update_dt 
	from de12.buma_stg_accounts stg
	inner join de12.buma_dwh_dim_accounts tgt
		on stg.account = tgt.account_num
		and tgt.end_dt = to_date('9999-12-31','YYYY-MM-DD')
	where stg.valid_to <> tgt.valid_to or ( stg.valid_to is null and tgt.valid_to is not null ) or ( stg.valid_to is not null and tgt.valid_to is null ) or 
		  stg.client <> tgt.client or ( stg.client is null and tgt.client is not null ) or ( stg.client is not null and tgt.client is null )
) tmp
where buma_dwh_dim_accounts.account_num = tmp.account
  and buma_dwh_dim_accounts.end_dt = to_date('9999-12-31','YYYY-MM-DD');
insert into de12.buma_dwh_dim_accounts (account_num, valid_to, client, start_dt, end_dt, deleted_flg)
select 
	stg.account, 
	stg.valid_to,
	stg.client,
	stg.update_dt,
	to_date('9999-12-31','YYYY-MM-DD'),
	'N'
from de12.buma_stg_accounts stg
inner join de12.buma_dwh_dim_accounts tgt
	on stg.account = tgt.account_num
	and tgt.end_dt = update_dt - interval '1 day'
where stg.valid_to <> tgt.valid_to or ( stg.valid_to is null and tgt.valid_to is not null ) or ( stg.valid_to is not null and tgt.valid_to is null ) or 
	  stg.client <> tgt.client or ( stg.client is null and tgt.client is not null ) or ( stg.client is not null and tgt.client is null );
insert into de12.buma_dwh_dim_accounts(account_num, valid_to, client, start_dt, end_dt, deleted_flg)
select 
	tgt.account_num,
	tgt.valid_to,
	tgt.client,
	now(),
	to_date('9999-12-31','YYYY-MM-DD'),
	'Y'
from de12.buma_dwh_dim_accounts tgt
left join de12.buma_stg_accounts_del stg
	on stg.account = tgt.account_num
where stg.account is null
  and tgt.end_dt = to_date('9999-12-31','YYYY-MM-DD')
  and tgt.deleted_flg = 'N';
update de12.buma_dwh_dim_accounts
set 
	end_dt = now() - interval '1 day'
where account_num in (
	select tgt.account_num
	from de12.buma_dwh_dim_accounts tgt
	left join de12.buma_stg_accounts_del stg
		on tgt.account_num = stg.account
	where stg.account is Null
	  and tgt.end_dt = to_date('9999-12-31','YYYY-MM-DD')
      and tgt.deleted_flg = 'N');
update de12.buma_meta
set max_update_dt = coalesce( (select max( update_dt ) from de12.buma_stg_accounts ), max_update_dt)
where schema_name='info' and table_name = 'accounts';
