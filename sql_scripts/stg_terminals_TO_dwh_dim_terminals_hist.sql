insert into de12.buma_dwh_dim_terminals_hist (terminal_id, terminal_type, terminal_city, terminal_address, effective_from, effective_to, deleted_flg)
select
	stg.terminal_id,
	stg.terminal_type,
	stg.terminal_city,
	stg.terminal_address,
	now()::date,
	to_date('2999-12-31','YYYY-MM-DD'),
	'N'
from de12.buma_stg_terminals stg
left join de12.buma_dwh_dim_terminals_hist tgt
on stg.terminal_id  = tgt.terminal_id
where tgt.terminal_id is null;
update de12.buma_dwh_dim_terminals_hist
set
	effective_to = (select max(transaction_date) from de12.buma_stg_transactions)::date - interval '1 day'
from (
	select
		stg.terminal_id
	from de12.buma_stg_terminals stg
	inner join de12.buma_dwh_dim_terminals_hist tgt
		on stg.terminal_id = tgt.terminal_id
		and tgt.effective_to = to_date('2999-12-31','YYYY-MM-DD')
	where stg.terminal_type <> tgt.terminal_type or ( stg.terminal_type is null and tgt.terminal_type is not null ) or ( stg.terminal_type is not null and tgt.terminal_type is null ) or
		  stg.terminal_city <> tgt.terminal_city or ( stg.terminal_city is null and tgt.terminal_city is not null ) or ( stg.terminal_city is not null and tgt.terminal_city is null ) or
		  stg.terminal_address <> tgt.terminal_address or ( stg.terminal_address is null and tgt.terminal_address is not null ) or ( stg.terminal_address is not null and tgt.terminal_address is null )
) tmp
where buma_dwh_dim_terminals_hist.terminal_id = tmp.terminal_id
  and buma_dwh_dim_terminals_hist.effective_to = to_date('2999-12-31','YYYY-MM-DD');
insert into de12.buma_dwh_dim_terminals_hist (terminal_id, terminal_type, terminal_city, terminal_address, effective_from, effective_to, deleted_flg)
select
	stg.terminal_id,
	stg.terminal_type,
	stg.terminal_city,
	stg.terminal_address,
	(select max(transaction_date) from de12.buma_stg_transactions)::date,
	to_date('2999-12-31','YYYY-MM-DD'),
	'N'
from de12.buma_stg_terminals stg
inner join de12.buma_dwh_dim_terminals_hist tgt
	on stg.terminal_id = tgt.terminal_id
	and tgt.effective_to = (now() - interval '1 day')::date
	where stg.terminal_type <> tgt.terminal_type or ( stg.terminal_type is null and tgt.terminal_type is not null ) or ( stg.terminal_type is not null and tgt.terminal_type is null ) or
		  stg.terminal_city <> tgt.terminal_city or ( stg.terminal_city is null and tgt.terminal_city is not null ) or ( stg.terminal_city is not null and tgt.terminal_city is null ) or
		  stg.terminal_address <> tgt.terminal_address or ( stg.terminal_address is null and tgt.terminal_address is not null ) or ( stg.terminal_address is not null and tgt.terminal_address is null );
insert into de12.buma_dwh_dim_terminals_hist (terminal_id, terminal_type, terminal_city, terminal_address, effective_from, effective_to, deleted_flg)
select
	tgt.terminal_id,
	tgt.terminal_type,
	tgt.terminal_city,
	tgt.terminal_address,
	(select max(transaction_date) from de12.buma_stg_transactions)::date,
	to_date('2999-12-31','YYYY-MM-DD'),
	'Y'
from de12.buma_dwh_dim_terminals_hist tgt
left join de12.buma_stg_terminals stg
	on tgt.terminal_id = stg.terminal_id
where stg.terminal_id is null
  and tgt.effective_to = to_date('2999-12-31','YYYY-MM-DD')
  and tgt.deleted_flg = 'N';
update de12.buma_dwh_dim_terminals_hist
set
	effective_to = (select max(transaction_date) from de12.buma_stg_transactions)::date - interval '1 day'
where terminal_id in (
	select tgt.terminal_id
	from de12.buma_dwh_dim_terminals_hist tgt
	left join de12.buma_stg_terminals stg
		on stg.terminal_id = tgt.terminal_id
	where stg.terminal_id is Null
	  and tgt.effective_to = to_date('2999-12-31','YYYY-MM-DD')
      and tgt.deleted_flg = 'N');
insert into de12.buma_dwh_dim_terminals_hist (terminal_id, terminal_type, terminal_city, terminal_address, effective_from, effective_to, deleted_flg)
select
	stg.terminal_id,
	stg.terminal_type,
	stg.terminal_city,
	stg.terminal_address,
	now()::date,
	to_date('2999-12-31','YYYY-MM-DD'),
	'N'
from de12.buma_stg_terminals stg
left join de12.buma_dwh_dim_terminals_hist tgt
on stg.terminal_id  = tgt.terminal_id
where tgt.terminal_id is null;
update de12.buma_dwh_dim_terminals_hist
set
	effective_to = (select max(transaction_date) from de12.buma_stg_transactions)::date - interval '1 day'
from (
	select
		stg.terminal_id
	from de12.buma_stg_terminals stg
	inner join de12.buma_dwh_dim_terminals_hist tgt
		on stg.terminal_id = tgt.terminal_id
		and tgt.effective_to = to_date('2999-12-31','YYYY-MM-DD')
	where stg.terminal_type <> tgt.terminal_type or ( stg.terminal_type is null and tgt.terminal_type is not null ) or ( stg.terminal_type is not null and tgt.terminal_type is null ) or
		  stg.terminal_city <> tgt.terminal_city or ( stg.terminal_city is null and tgt.terminal_city is not null ) or ( stg.terminal_city is not null and tgt.terminal_city is null ) or
		  stg.terminal_address <> tgt.terminal_address or ( stg.terminal_address is null and tgt.terminal_address is not null ) or ( stg.terminal_address is not null and tgt.terminal_address is null )
) tmp
where buma_dwh_dim_terminals_hist.terminal_id = tmp.terminal_id
  and buma_dwh_dim_terminals_hist.effective_to = to_date('2999-12-31','YYYY-MM-DD');
insert into de12.buma_dwh_dim_terminals_hist (terminal_id, terminal_type, terminal_city, terminal_address, effective_from, effective_to, deleted_flg)
select
	stg.terminal_id,
	stg.terminal_type,
	stg.terminal_city,
	stg.terminal_address,
	(select max(transaction_date) from de12.buma_stg_transactions)::date,
	to_date('2999-12-31','YYYY-MM-DD'),
	'N'
from de12.buma_stg_terminals stg
inner join de12.buma_dwh_dim_terminals_hist tgt
	on stg.terminal_id = tgt.terminal_id
	and tgt.effective_to = (now() - interval '1 day')::date
	where stg.terminal_type <> tgt.terminal_type or ( stg.terminal_type is null and tgt.terminal_type is not null ) or ( stg.terminal_type is not null and tgt.terminal_type is null ) or
		  stg.terminal_city <> tgt.terminal_city or ( stg.terminal_city is null and tgt.terminal_city is not null ) or ( stg.terminal_city is not null and tgt.terminal_city is null ) or
		  stg.terminal_address <> tgt.terminal_address or ( stg.terminal_address is null and tgt.terminal_address is not null ) or ( stg.terminal_address is not null and tgt.terminal_address is null );
insert into de12.buma_dwh_dim_terminals_hist (terminal_id, terminal_type, terminal_city, terminal_address, effective_from, effective_to, deleted_flg)
select
	tgt.terminal_id,
	tgt.terminal_type,
	tgt.terminal_city,
	tgt.terminal_address,
	(select max(transaction_date) from de12.buma_stg_transactions)::date,
	to_date('2999-12-31','YYYY-MM-DD'),
	'Y'
from de12.buma_dwh_dim_terminals_hist tgt
left join de12.buma_stg_terminals stg
	on tgt.terminal_id = stg.terminal_id
where stg.terminal_id is null
  and tgt.effective_to = to_date('2999-12-31','YYYY-MM-DD')
  and tgt.deleted_flg = 'N';
update de12.buma_dwh_dim_terminals_hist
set
	effective_to = (select max(transaction_date) from de12.buma_stg_transactions)::date - interval '1 day'
where terminal_id in (
	select tgt.terminal_id
	from de12.buma_dwh_dim_terminals_hist tgt
	left join de12.buma_stg_terminals stg
		on stg.terminal_id = tgt.terminal_id
	where stg.terminal_id is Null
and tgt.effective_to = to_date('2999-12-31','YYYY-MM-DD')
and tgt.deleted_flg = 'N');