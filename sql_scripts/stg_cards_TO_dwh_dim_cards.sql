insert into de12.buma_dwh_dim_cards
	select
		card_num,
		account
	from de12.buma_stg_cards;