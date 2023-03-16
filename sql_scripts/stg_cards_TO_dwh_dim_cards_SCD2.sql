insert into de12.buma_dwh_dim_cards (card_num, account_num, start_dt, end_dt, deleted_flg)
select 
	stg.card_num,
	stg.account,
	stg.create_dt, 
	to_date('2999-12-31','YYYY-MM-DD'),
	'N'
from de12.buma_stg_cards stg
left join de12.buma_dwh_dim_cards tgt
on stg.card_num  = tgt.card_num
where tgt.card_num is null;
update de12.buma_dwh_dim_cards
set 
	end_dt = tmp.update_dt - interval '1 day'
from (
	select 
		stg.card_num, 
		stg.update_dt 
	from de12.buma_stg_cards stg
	inner join de12.buma_dwh_dim_cards tgt
		on stg.card_num = tgt.card_num
		and tgt.end_dt = to_date('2999-12-31','YYYY-MM-DD')
	where stg.account <> tgt.account_num or ( stg.account is null and tgt.account_num is not null ) or ( stg.account is not null and tgt.account_num is null )
) tmp
where buma_dwh_dim_cards.card_num = tmp.card_num
  and buma_dwh_dim_cards.end_dt = to_date('2999-12-31','YYYY-MM-DD');
insert into de12.buma_dwh_dim_cards (card_num, account_num, start_dt, end_dt, deleted_flg)
select 
	stg.card_num, 
	stg.account,
	stg.update_dt,
	to_date('2999-12-31','YYYY-MM-DD'),
	'N'
from de12.buma_stg_cards stg
inner join de12.buma_dwh_dim_cards tgt
	on stg.card_num = tgt.card_num
	and tgt.end_dt = update_dt - interval '1 day'
where stg.account <> tgt.account_num or ( stg.account is null and tgt.account_num is not null ) or ( stg.account is not null and tgt.account_num is null );
insert into de12.buma_dwh_dim_cards(card_num, account_num, start_dt, end_dt, deleted_flg)
select 
	tgt.card_num,
	tgt.account_num,
	now(),
	to_date('2999-12-31','YYYY-MM-DD'),
	'Y'
from de12.buma_dwh_dim_cards tgt
    left join de12.buma_stg_cards_del stg
	on stg.card_num = tgt.card_num
where stg.card_num is null
  and tgt.end_dt = to_date('2999-12-31','YYYY-MM-DD')
  and tgt.deleted_flg = 'N';
update de12.buma_dwh_dim_cards
set 
	end_dt = now() - interval '1 day'
where card_num in (
	select tgt.card_num
	from de12.buma_dwh_dim_cards tgt
	    left join de12.buma_stg_cards_del stg
		on tgt.card_num = stg.card_num
	where stg.card_num is Null
	  and tgt.end_dt = to_date('2999-12-31','YYYY-MM-DD')
      and tgt.deleted_flg = 'N');
update de12.buma_meta_stg
set max_update_dt = coalesce( 
	(select 
	  case 
	    when max(create_dt) > max(coalesce(update_dt, to_date('1899-01-01', 'YYYY-MM-DD'))) then max(create_dt)
	  else max(update_dt)
	  end
	from de12.buma_stg_cards 
	), max_update_dt)
where schema_name='info' and table_name = 'cards';
