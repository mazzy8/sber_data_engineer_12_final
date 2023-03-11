----------------------------------------------------------------------------
-- STAGE

create table de12.buma_stg_transaction (
	trans_id varchar(20),
	trans_date timestamp(0),
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

create table de12.buma_stg_cards (
	card_num varchar(20),
	account bpchar(20),
	create_dt timestamp(0),
	update_dt timestamp(0)
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

create table de12.buma_stg_meta(
    schema_name varchar(30),
    table_name varchar(30),
    max_update_dt timestamp(0)
);

insert into de12.buma_stg_meta( schema_name, table_name, max_update_dt )
values( 'info','accounts', to_timestamp('1900-01-01','YYYY-MM-DD')),
	   ('info','clients', to_timestamp('1900-01-01','YYYY-MM-DD')),
	   ('info','cards', to_timestamp('1900-01-01','YYYY-MM-DD')
);


----------------------------------------------------------------------------
-- DETAIL

create table de12.buma_dwh_dim_terminals (
	terminal_id varchar(10),
	terminal_type varchar(4),
	terminal_city varchar(20),
	terminal_address varchar(100)
);

create table de12.buma_dwh_dim_cards (
	card_num varchar(20),
	account bpchar(20)
);

create table de12.buma_dwh_dim_accounts (
	account_num varchar(20),
	valid_to date,
	client varchar(10)
);

CREATE TABLE de12.buma_dwh_dim_clients (
	client_id varchar(10),
	last_name varchar(20),
	first_name varchar(20),
	patronymic varchar(20),
	date_of_birth date,
	passport_num varchar(15),
	passport_valid_to date,
	phone varchar(16)
);

create table de12.buma_fact_passport_blacklist (
	date date,
	passport varchar(15)
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