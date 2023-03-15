----------------------------------------------------------------------------
-- STAGE

create table de12.buma_stg_transaction (
	transaction_id varchar(20),
	transaction_date timestamp(0),
	amount varchar(20),
	card_num varchar(20),
	oper_type varchar(10),
	oper_result varchar(10),
	terminal varchar(10)
);

create table de12.buma_stg_terminals (
	terminal_id varchar(10),
	terminal_type varchar(4),
	terminal_city varchar(20),
	terminal_address varchar(100)
);

create table de12.buma_stg_terminals_del(
	account varchar(20)
);

create table de12.buma_stg_passport_blacklist (
	date date,
	passport varchar(15)
);

CREATE TABLE de12.buma_stg_accounts (
	account varchar(20),
	valid_to date,
	client varchar(10),
	create_dt timestamp(0),
	update_dt timestamp(0)
);

create table de12.buma_stg_accounts_del(
	account varchar(20)
);

create table de12.buma_stg_cards (
	card_num varchar(20),
	account bpchar(20),
	create_dt timestamp(0),
	update_dt timestamp(0)
);

create table de12.buma_stg_cards_del(
	card_num varchar(20)
);

CREATE TABLE de12.buma_stg_clients (
	client_id varchar(10),
	last_name varchar(20),
	first_name varchar(20),
	patronymic varchar(20),
	date_of_birth date,
	passport_num varchar(15),
	passport_valid_to date,
	phone varchar(16),
	create_dt timestamp(0),
	update_dt timestamp(0)
);

CREATE TABLE de12.buma_stg_clients_del (
	client_id varchar(10)
);

create table de12.buma_meta_stg(
    schema_name varchar(30),
    table_name varchar(30),
    max_update_dt timestamp(0)
);

insert into de12.buma_meta_stg ( schema_name, table_name, max_update_dt )
values( 'info','accounts', to_timestamp('1900-01-01','YYYY-MM-DD')),
	   ('info','clients', to_timestamp('1900-01-01','YYYY-MM-DD')),
	   ('info','cards', to_timestamp('1900-01-01','YYYY-MM-DD')
);

create table de12.buma_meta_fraud (max_update_dt timestamp(0));

insert into de12.buma_meta_fraud ( max_update_dt )
values( to_timestamp('2999-12-31','YYYY-MM-DD'));


----------------------------------------------------------------------------
-- DETAIL

create table de12.buma_dwh_dim_terminals (
	terminal_id varchar(10),
	terminal_type varchar(4),
	terminal_city varchar(20),
	terminal_address varchar(100),
	start_dt timestamp(0),
	end_dt timestamp(0),
	deleted_flg char
);

create table de12.buma_dwh_dim_cards (
	card_num varchar(20),
	account_num varchar(20),
	start_dt timestamp(0),
	end_dt timestamp(0),
	deleted_flag char
);

create table de12.buma_dwh_dim_accounts (
	account_num varchar(20),
	valid_to date,
	client varchar(10),
	start_dt timestamp(0),
	end_dt timestamp(0),
	deleted_flg char
);

create table de12.buma_dwh_dim_clients (
	client_id varchar(10),
	last_name varchar(20),
	first_name varchar(20),
	patronymic varchar(20),
	date_of_birth date,
	passport_num varchar(15),
	passport_valid_to date,
	phone varchar(16),
	start_dt timestamp(0),
	end_dt timestamp(0),
	deleted_flg char
);

create table de12.buma_dwh_fact_passport_blacklist (
	date date,
	passport_num varchar(15)
);

create table de12.buma_dwh_fact_transactions (
	trans_id varchar(20),
	trans_date timestamp(0),
	card_num varchar(20),
	oper_type varchar(10),
	amt decimal(18,2),
	oper_result varchar(10),
	terminal varchar(10)
);

CREATE TABLE de12.buma_rep_fraud (
	event_dt date,
	passport varchar(15),
	fio varchar(100),
	phone varchar(16),
	event_type int,
	report_dt date
);
