with all_tables_for_T1_and_T2 as(
    select *
    from de12.buma_dwh_fact_transactions tran
        left join de12.buma_dwh_dim_cards_hist card
        on tran.card_num = card.card_num
            left join de12.buma_dwh_dim_accounts_hist acc
            on card.account_num = acc.account_num
                left join de12.buma_dwh_dim_clients_hist cli
                on acc.client = cli.client_id
    where tran.trans_date > (select min(transaction_date::timestamp) from de12.buma_stg_transactions)
),
Type_1 as(
    select
        allt.trans_date as event_dt,
        allt.passport_num as passport,
        concat(allt.last_name, ' ', allt.first_name, ' ', allt.patronymic) as fio,
        allt.phone as phone,
        1 event_type,
        now()::date as report_dt
    from all_tables_for_T1_and_T2 as allt
    where allt.passport_num in (select passport_num from de12.buma_dwh_fact_passport_blacklist)
        or allt.passport_num in (select passport_num from all_tables_for_T1_and_T2 where passport_valid_to is not null and passport_valid_to < trans_date::date )
),
Type_2 as (
    select
        allt.trans_date as event_dt,
        allt.passport_num passport,
        concat(allt.last_name, ' ', allt.first_name, ' ', allt.patronymic) as fio,
        allt.phone as phone,
        2 as event_type,
        now()::date as report_dt
        from all_tables_for_T1_and_T2 as allt
        where valid_to < trans_date::date
),
Type_3_diff_city as(
    select
        card_num,
        count(distinct term.terminal_city) as cnt_city
    from de12.buma_dwh_fact_transactions trans
	    left join de12.buma_dwh_dim_terminals_hist term
	    on trans.terminal = term.terminal_id
	where term.terminal_city is not null
		and trans.trans_date > (select min(transaction_date::timestamp) from de12.buma_stg_transactions) - 60 * interval'1 minute'
    group by trans.card_num
    having count(distinct term.terminal_city) > 1
),
Type_3_trans_per_city as(
    select
        trans.card_num,
        trans.trans_date,
        term.terminal_city
    from de12.buma_dwh_fact_transactions trans
	    inner join Type_3_diff_city df
	    on trans.card_num = df.card_num
    		left join de12.buma_dwh_dim_terminals_hist term
    		on trans.terminal = term.terminal_id
    where trans.trans_date > (select min(transaction_date::timestamp) from de12.buma_stg_transactions) - 60 * interval'1 minute'
),
Type_3_trans_last_and_current_city as(
    select
        card_num,
        terminal_city as current_city,
        lead(terminal_city) over(partition by card_num order by trans_date) as second_city,
        lead(trans_date) over(partition by card_num order by trans_date) as second_city_date,
        lead(terminal_city, 2) over(partition by card_num order by trans_date) as third_city,
        lead(trans_date, 2) over(partition by card_num order by trans_date) as third_city_date
    from Type_3_trans_per_city
),
Type_3_fraud as(
    select
        card_num,
        min(third_city_date) as trans_date
    from Type_3_trans_last_and_current_city
    where extract(epoch FROM (third_city_date - second_city_date)/60) < 60
    and current_city = second_city
    and second_city is not Null
    and second_city != third_city
    and third_city is not Null
    group by card_num
),
Type_3 as(
    select
        fr.trans_date as event_dt,
        clients.passport_num as passport,
        concat(clients.last_name, ' ', clients.first_name, ' ', clients.patronymic) as fio,
        clients.phone,
	    3 as event_type,
        now()::date as report_dt
    from Type_3_fraud fr
    	left join de12.buma_dwh_dim_cards_hist cards
    	on fr.card_num = cards.card_num
    		left join de12.buma_dwh_dim_accounts_hist accounts
    		on cards.account_num = accounts.account_num
    			left join de12.buma_dwh_dim_clients_hist clients
    			on accounts.client = clients.client_id
),
Type_4_data_preparation_for_sampling as(
  	select
	    card_num,
	    trans_date,
	    oper_result,
	    oper_type,
	    amt,
	    LAG(oper_result, 1) over (partition by card_num order by trans_date) as previous_result_1,
	    LAG(oper_result, 2) over (partition by card_num order by trans_date) as previous_result_2,
	    LAG(oper_result, 3) over (partition by card_num order by trans_date) as previous_result_3,
	    LAG(amt, 1) over (partition by card_num order by trans_date) as previous_amt_1,
	    LAG(amt, 2) over (partition by card_num order by trans_date) as previous_amt_2,
	    LAG(amt, 3) over (partition by card_num order by trans_date) as previous_amt_3,
	    LAG(trans_date, 3) over (partition by card_num order by trans_date) as previous_date_3
	from de12.buma_dwh_fact_transactions
	where trans_date > (select min(transaction_date::timestamp) from de12.buma_stg_transactions) - 20 * interval'1 minute'
),
Type_4_sample as(
	select
	    card_num,
	    trans_date
	from Type_4_data_preparation_for_sampling
	where
	    oper_result = 'SUCCESS'
	    and previous_result_1 = 'REJECT'
	    and previous_result_2 = 'REJECT'
	    and previous_result_3 = 'REJECT'
	    and previous_amt_3 > previous_amt_2
	    and previous_amt_3 is not Null
	    and previous_amt_2 > previous_amt_1
	    and previous_amt_2 is not Null
	    and previous_amt_1 > amt
	    and previous_amt_1 is not Null
	    and extract(epoch FROM (trans_date - previous_date_3)/60) < 20
),
Type_4 as(
    select
        samp.trans_date as event_dt,
        clients.passport_num as passport,
        concat(clients.last_name, ' ', clients.first_name, ' ', clients.patronymic) as fio,
	clients.phone as phone,
	4 as event_type,
        now()::date as report_dt
    from Type_4_sample samp
    	left join de12.buma_dwh_dim_cards_hist cards 
    	on samp.card_num = cards.card_num
    		left join de12.buma_dwh_dim_accounts_hist accounts 
    		on cards.account_num = accounts.account_num
    			left join de12.buma_dwh_dim_clients_hist clients 
    			on accounts.client = clients.client_id
),
all_frauds as(
    (select * from Type_1)
    union
    (select * from Type_2)
    union
    (select * from Type_3)
    union
    (select * from Type_4)
    )
insert into de12.buma_rep_fraud
    select * from all_frauds;