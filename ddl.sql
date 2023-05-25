-- Создаем 2 базы данных и схемы для них, предполагая что слои stg и dds будут на разных серверах
CREATE DATABASE stg_db;
CREATE DATABASE dds_db;

CREATE SCHEMA stg;
CREATE SCHEMA dds;

-- Единственная таблица в слое stg
CREATE TABLE stg.posts (
post_id SERIAL PRIMARY KEY,
bk_post_id INT,
bk_post_id_hash_key CHAR(40),
bk_user_id INT,
bk_user_id_hash_key CHAR(40),
title TEXT,
body TEXT,
load_date TIMESTAMP
);

-- Создаем view с подключение к stg слою

CREATE EXTENSION dblink;

CREATE VIEW dds.posts AS 
 SELECT tbl.post_id,
    tbl.bk_post_id,
    tbl.bk_post_id_hash_key,
    tbl.bk_user_id,
    tbl.bk_user_id_hash_key,
    tbl.title,
    tbl.body,
    tbl.load_date
   FROM dblink('host=0.0.0.0 port=5432 user=root password=root dbname=stg_db'::text, 'SELECT * FROM stg.posts'::text) 
   tbl(post_id integer, bk_post_id integer, bk_post_id_hash_key character(40), bk_user_id integer, 
   bk_user_id_hash_key character(40), title text, body text, load_date timestamp without time zone);

--Создаем таблицы в dds

CREATE TABLE dds.hub_post (
	hub_post_id serial PRIMARY KEY ,
	bk_post_id_hash_key char(40),
	load_date timestamp
);

CREATE TABLE dds.hub_user (
	hub_user_id serial PRIMARY KEY ,
	bk_user_id_hash_key char(40),
	load_date timestamp
);

CREATE TABLE dds.link_publications (
	link_publications_id serial PRIMARY KEY,
	bk_post_id_hash_key char(40),
	bk_user_id_hash_key char(40),
	load_date timestamp
);

CREATE TABLE dds.satellite_post (
	satellite_post_id serial PRIMARY KEY,
	bk_post_id_hash_key char(40),
	title text,
	body text,
	load_date timestamp
);

CREATE TABLE dds.satellite_user (
	satellite_user_id serial PRIMARY KEY,
	bk_user_id_hash_key char(40),
	load_date timestamp
);

--Создаем функцию для формирования детального слоя

CREATE OR REPLACE FUNCTION dds.stg_to_dds()
RETURNS void AS $$
BEGIN
	INSERT INTO dds.hub_post(bk_post_id_hash_key,load_date)
	SELECT DISTINCT posts.bk_post_id_hash_key,posts.load_date  FROM dds.posts
	LEFT JOIN dds.hub_post AS hub_post USING(bk_post_id_hash_key)
	WHERE hub_post.bk_post_id_hash_key IS NULL;
	
	INSERT INTO dds.hub_user(bk_user_id_hash_key,load_date)
	SELECT DISTINCT posts.bk_user_id_hash_key,posts.load_date  FROM dds.posts
	LEFT JOIN dds.hub_user AS hub_user USING(bk_user_id_hash_key)
	WHERE hub_user.bk_user_id_hash_key IS NULL;
	
	INSERT INTO dds.link_publications(bk_post_id_hash_key,bk_user_id_hash_key,load_date)
	SELECT DISTINCT posts.bk_post_id_hash_key,posts.bk_user_id_hash_key,posts.load_date  FROM dds.posts
	LEFT JOIN dds.link_publications AS link_publications 
	USING(bk_post_id_hash_key,bk_user_id_hash_key) 
	WHERE link_publications.bk_user_id_hash_key IS NULL;
	
	INSERT INTO dds.satellite_post(bk_post_id_hash_key,title, body, load_date)
	SELECT DISTINCT posts.bk_post_id_hash_key,posts.title, posts.body, posts.load_date  FROM dds.posts;	
	
	INSERT INTO dds.satellite_user(bk_user_id_hash_key, load_date)
	SELECT DISTINCT posts.bk_user_id_hash_key, posts.load_date  FROM dds.posts;
END;
$$ LANGUAGE plpgsql;