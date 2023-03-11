with all_tables as(
	select *
	from de12.buma_dwh_fact_transactions tran
		left join de12.buma_dwh_dim_cards card
		on trim(tran.card_num) = trim(card.card_num)
			left join de12.buma_dwh_dim_accounts acc
			on card.account = acc.account_num
				left join de12.buma_dwh_dim_clients cli
				on acc.client = cli.client_id
),
T1 as(
	select *
	from all_tables
	where trans_date > valid_to and lower(oper_result) = 'success'
),
T2 as(
	select *
	from all_tables
	where trim(passport_num) in (select trim(passport) from de12.buma_fact_passport_blacklist) and lower(oper_result) = 'success'
),
T3 as(
	select *
	from all_tables
	where lower(oper_result) = 'success'
	order by trim(card_num), trans_date
)
select *
from all_tables