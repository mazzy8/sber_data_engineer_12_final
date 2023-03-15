with allt as(
  select *
  from de12.buma_dwh_fact_transactions tran
    left join de12.buma_dwh_dim_cards card
    on tran.card_num = card.card_num
      left join de12.buma_dwh_dim_accounts acc
      on card.account_num = acc.account_num
        left join de12.buma_dwh_dim_clients cli
        on acc.client = cli.client_id
  where tran.trans_date > (select max_update_dt from de12.buma_meta_fraud)
),
T1 as(
  select
	allt.trans_date event_dt,
	allt.passport_num passport,
	concat(allt.last_name, ' ', allt.first_name, ' ', allt.patronymic) fio,
	allt.phone phone,
	1 event_type,
	now()::date report_dt
  from allt
  where lower(allt.oper_result) = 'success' and (allt.passport_num in (select bl.passport_num from de12.buma_dwh_fact_passport_blacklist bl ) or
  allt.passport_num in (select passport_num from allt where passport_valid_to is not null and passport_valid_to < trans_date ))
),
T2 as (
  select
	allt.trans_date event_dt,
	allt.passport_num passport,
	concat(allt.last_name, ' ', allt.first_name, ' ', allt.patronymic) fio,
	allt.phone phone,
	2 event_type,
	now()::date report_dt
  from allt
  where valid_to < trans_date and lower(allt.oper_result) = 'success'
),
T3 as(
  select
  *
  from allt
    left join de12.buma_dwh_dim_terminals term
    on allt.terminal = term.terminal_id
  where lower(allt.oper_result) = 'success'
  order by trans_date
),
all_frauds as(
  (select * from T1)
  union
  (select * from T2)
)
insert into de12.buma_rep_fraud
	select * from all_frauds;
update de12.buma_meta_fraud
set max_update_dt = coalesce( (select max( trans_date ) from de12.buma_stg_transactions ), max_update_dt);