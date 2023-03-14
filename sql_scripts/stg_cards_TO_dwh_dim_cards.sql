insert into de12.buma_dwh_dim_cards
	select
		card_num,
		account_num
	from de12.buma_stg_cards;
