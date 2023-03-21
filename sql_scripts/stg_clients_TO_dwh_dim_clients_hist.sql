insert into de12.buma_dwh_dim_clients_hist (client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, effective_from, effective_to, deleted_flg)
select
	stg.client_id,
	stg.last_name,
	stg.first_name,
	stg.patronymic,
	stg.date_of_birth,
	stg.passport_num,
	stg.passport_valid_to,
	stg.phone,
	stg.create_dt,
	to_date('2999-12-31','YYYY-MM-DD'),
	'N'
from de12.buma_stg_clients stg
left join de12.buma_dwh_dim_clients_hist tgt
on stg.client_id  = tgt.client_id
where tgt.client_id is null;
update de12.buma_dwh_dim_clients_hist
set
	effective_to = tmp.update_dt - interval '1 day'
from (
	select
		stg.client_id,
		stg.update_dt
	from de12.buma_stg_clients stg
	inner join de12.buma_dwh_dim_clients_hist tgt
		on stg.client_id = tgt.client_id
		and tgt.effective_to = to_date('2999-12-31','YYYY-MM-DD')
	where stg.last_name <> tgt.last_name or ( stg.last_name is null and tgt.last_name is not null ) or ( stg.last_name is not null and tgt.last_name is null ) or
		  stg.patronymic <> tgt.patronymic or ( stg.patronymic is null and tgt.patronymic is not null ) or ( stg.patronymic is not null and tgt.patronymic is null ) or
		  stg.date_of_birth <> tgt.date_of_birth or ( stg.date_of_birth is null and tgt.date_of_birth is not null ) or ( stg.date_of_birth is not null and tgt.date_of_birth is null ) or
		  stg.first_name <> tgt.first_name or ( stg.first_name is null and tgt.first_name is not null ) or ( stg.first_name is not null and tgt.first_name is null ) or
		  stg.passport_num <> tgt.passport_num or ( stg.passport_num is null and tgt.passport_num is not null ) or ( stg.passport_num is not null and tgt.passport_num is null ) or
		  stg.passport_valid_to <> tgt.passport_valid_to or ( stg.passport_valid_to is null and tgt.passport_valid_to is not null ) or ( stg.passport_valid_to is not null and tgt.passport_valid_to is null ) or
		  stg.passport_valid_to <> tgt.passport_valid_to or ( stg.passport_valid_to is null and tgt.passport_valid_to is not null ) or ( stg.passport_valid_to is not null and tgt.passport_valid_to is null )
) tmp
where buma_dwh_dim_clients_hist.client_id = tmp.client_id
  and buma_dwh_dim_clients_hist.effective_to = to_date('2999-12-31','YYYY-MM-DD');
insert into de12.buma_dwh_dim_clients_hist (client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, effective_from, effective_to, deleted_flg)
select
	stg.client_id,
	stg.last_name,
	stg.first_name,
	stg.patronymic,
	stg.date_of_birth,
	stg.passport_num,
	stg.passport_valid_to,
	stg.phone,
	stg.update_dt,
	to_date('2999-12-31','YYYY-MM-DD'),
	'N'
from de12.buma_stg_clients stg
inner join de12.buma_dwh_dim_clients_hist tgt
	on stg.client_id = tgt.client_id
	and tgt.effective_to = update_dt - interval '1 day'
where stg.last_name <> tgt.last_name or ( stg.last_name is null and tgt.last_name is not null ) or ( stg.last_name is not null and tgt.last_name is null ) or
		  stg.patronymic <> tgt.patronymic or ( stg.patronymic is null and tgt.patronymic is not null ) or ( stg.patronymic is not null and tgt.patronymic is null ) or
		  stg.date_of_birth <> tgt.date_of_birth or ( stg.date_of_birth is null and tgt.date_of_birth is not null ) or ( stg.date_of_birth is not null and tgt.date_of_birth is null ) or
		  stg.first_name <> tgt.first_name or ( stg.first_name is null and tgt.first_name is not null ) or ( stg.first_name is not null and tgt.first_name is null ) or
		  stg.passport_num <> tgt.passport_num or ( stg.passport_num is null and tgt.passport_num is not null ) or ( stg.passport_num is not null and tgt.passport_num is null ) or
		  stg.passport_valid_to <> tgt.passport_valid_to or ( stg.passport_valid_to is null and tgt.passport_valid_to is not null ) or ( stg.passport_valid_to is not null and tgt.passport_valid_to is null ) or
		  stg.passport_valid_to <> tgt.passport_valid_to or ( stg.passport_valid_to is null and tgt.passport_valid_to is not null ) or ( stg.passport_valid_to is not null and tgt.passport_valid_to is null );
insert into de12.buma_dwh_dim_clients_hist (client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, effective_from, effective_to, deleted_flg)
select
	tgt.client_id,
	tgt.last_name,
	tgt.first_name,
	tgt.patronymic,
	tgt.date_of_birth,
	tgt.passport_num,
	tgt.passport_valid_to,
	tgt.phone,
	now(),
	to_date('2999-12-31','YYYY-MM-DD'),
	'Y'
from de12.buma_dwh_dim_clients_hist tgt
left join de12.buma_stg_clients_del stg
	on stg.client_id = tgt.client_id
where stg.client_id is null
  and tgt.effective_to = to_date('2999-12-31','YYYY-MM-DD')
  and tgt.deleted_flg = 'N';
update de12.buma_dwh_dim_clients_hist
set
	effective_to = now() - interval '1 day'
where client_id in (
	select tgt.client_id
	from de12.buma_dwh_dim_clients_hist tgt
	left join de12.buma_stg_clients_del stg
		on tgt.client_id = stg.client_id
	where stg.client_id is Null
	  and tgt.effective_to = to_date('2999-12-31','YYYY-MM-DD')
      and tgt.deleted_flg = 'N');
update de12.buma_meta_stg
set max_update_dt = coalesce(
	(select
	  case
	    when max(create_dt) > max(coalesce(update_dt, to_date('1899-01-01', 'YYYY-MM-DD'))) then max(create_dt)
	  else max(update_dt)
	  end
	from de12.buma_stg_clients
	), max_update_dt)
where schema_name='info' and table_name = 'clients';