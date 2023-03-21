----------------------------------------------------------------------------
-- STAGE

create table de12.buma_stg_transactions (
	transaction_id varchar(20),
	transaction_date varchar,
	amount varchar,
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
values( 'info','accounts', to_timestamp('1899-01-01','YYYY-MM-DD')),
	   ('info','clients', to_timestamp('1899-01-01','YYYY-MM-DD')),
	   ('info','cards', to_timestamp('1899-01-01','YYYY-MM-DD')
);

----------------------------------------------------------------------------
-- DETAIL

create table de12.buma_dwh_dim_terminals_hist (
	terminal_id varchar(10),
	terminal_type varchar(4),
	terminal_city varchar(20),
	terminal_address varchar(100),
	effective_from timestamp(0),
	effective_to timestamp(0),
	deleted_flg char
);

create table de12.buma_dwh_dim_cards_hist (
	card_num varchar(20),
	account_num varchar(20),
	effective_from timestamp(0),
	effective_to timestamp(0),
	deleted_flg char
);

create table de12.buma_dwh_dim_accounts_hist (
	account_num varchar(20),
	valid_to date,
	client varchar(10),
	effective_from timestamp(0),
	effective_to timestamp(0),
	deleted_flg char
);

create table de12.buma_dwh_dim_clients_hist (
	client_id varchar(10),
	last_name varchar(20),
	first_name varchar(20),
	patronymic varchar(20),
	date_of_birth date,
	passport_num varchar(15),
	passport_valid_to date,
	phone varchar(16),
	effective_from timestamp(0),
	effective_to timestamp(0),
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

----------------------------------------------------------------------------
-- DATA MARTS

CREATE TABLE de12.buma_rep_fraud (
	event_dt timestamp(0),
	passport varchar(15),
	fio varchar(100),
	phone varchar(16),
	event_type int,
	report_dt date
);


------------------------------------------------------------
--Clear
--drop table de12.buma_stg_transactions;
--drop table de12.buma_stg_terminals;
--drop table de12.buma_stg_passport_blacklist;
--drop table de12.buma_stg_accounts;
--drop table de12.buma_stg_accounts_del;
--drop table de12.buma_stg_cards;
--drop table de12.buma_stg_cards_del;
--drop table de12.buma_stg_clients;
--drop table de12.buma_stg_clients_del;
--drop table de12.buma_meta_stg;
--drop table de12.buma_dwh_dim_terminals_hist;
--drop table de12.buma_dwh_dim_cards_hist;
--drop table de12.buma_dwh_dim_accounts_hist;
--drop table de12.buma_dwh_dim_clients_hist;
--drop table de12.buma_dwh_fact_passport_blacklist;
--drop table de12.buma_dwh_fact_transactions;
--drop table de12.buma_rep_fraud;

--delete from de12.buma_stg_transactions;
--delete from de12.buma_stg_terminals;
--delete from de12.buma_stg_passport_blacklist;
--delete from de12.buma_stg_accounts;
--delete from de12.buma_stg_accounts_del;
--delete from de12.buma_stg_cards;
--delete from de12.buma_stg_cards_del;
--delete from de12.buma_stg_clients;
--delete from de12.buma_stg_clients_del;
--delete from de12.buma_meta_stg;
--delete from de12.buma_dwh_dim_terminals_hist;
--delete from de12.buma_dwh_dim_cards_hist;
--delete from de12.buma_dwh_dim_accounts_hist;
--delete from de12.buma_dwh_dim_clients_hist;
--delete from de12.buma_dwh_fact_passport_blacklist;
--delete from de12.buma_dwh_fact_transactions;
--delete from de12.buma_rep_fraud;
