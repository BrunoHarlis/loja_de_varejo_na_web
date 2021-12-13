------------------------------------------------------------
-- CRIANDO O DATABASE LOJA_WEB
------------------------------------------------------------
DROP DATABASE IF EXISTS loja_web CASCADE;

CREATE DATABASE loja_web;


------------------------------------------------------------
-- CRIANDO TABELA EXTERNA EX_USUARIOS
------------------------------------------------------------
DROP TABLE IF EXISTS loja_web.ex_usuarios;

CREATE EXTERNAL TABLE loja_web.ex_usuarios(
swid STRING,
aniversario STRING,
genero STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/tmp/loja_varejo/users'
TBLPROPERTIES("skip.header.line.count"="1");



------------------------------------------------------------
-- CRIANDO TABELA EXTERNA EX_PRODUTOS
------------------------------------------------------------
DROP TABLE IF EXISTS loja_web.ex_produtos;

CREATE EXTERNAL TABLE loja_web.ex_produtos(
url STRING,
categoria STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/tmp/loja_varejo/products'
TBLPROPERTIES("skip.header.line.count"="1");



------------------------------------------------------------
-- CRIANDO TABELA EXTERNA EX_OMNITURELOGS
------------------------------------------------------------
DROP TABLE IF EXISTS loja_web.ex_omniturelogs;

CREATE EXTERNAL TABLE loja_web.ex_omniturelogs(
col_1 STRING,col_2 STRING,col_3 STRING,col_4 STRING,col_5 STRING,
col_6 STRING,col_7 STRING,col_8 STRING,col_9 STRING,col_10 STRING,
col_11 STRING,col_12 STRING,col_13 STRING,col_14 STRING,col_15 STRING,
col_16 STRING,col_17 STRING,col_18 STRING,col_19 STRING,col_20 STRING,
col_21 STRING,col_22 STRING,col_23 STRING,col_24 STRING,col_25 STRING,
col_26 STRING,col_27 STRING,col_28 STRING,col_29 STRING,col_30 STRING,
col_31 STRING,col_32 STRING,col_33 STRING,col_34 STRING,col_35 STRING,
col_36 STRING,col_37 STRING,col_38 STRING,col_39 STRING,col_40 STRING,
col_41 STRING,col_42 STRING,col_43 STRING,col_44 STRING,col_45 STRING,
col_46 STRING,col_47 STRING,col_48 STRING,col_49 STRING,col_50 STRING,
col_51 STRING,col_52 STRING,col_53 STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/tmp/loja_varejo/omniture-logs'
TBLPROPERTIES("skip.header.line.count"="1");



------------------------------------------------------------
-- CRIANDO TABELAS USUARIOS
------------------------------------------------------------
DROP TABLE IF EXISTS loja_web.usuarios;

CREATE TABLE loja_web.usuario(
swid STRING,
aniversario STRING,
genero STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS PARQUET
TBLPROPERTIES("parquet.compression"="SNAPPY");



------------------------------------------------------------
-- CRIANDO TABELA PRODUTOS
------------------------------------------------------------
DROP TABLE IF EXISTS loja_web.produtos;

CREATE TABLE loja_web.produtos(
url STRING,
categoria STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS PARQUET
TBLPROPERTIES("parquet.compression"="SNAPPY");



------------------------------------------------------------
-- CRIANDO TABELA OMNITURELOGS
------------------------------------------------------------
DROP TABLE IF EXISTS loja_web.omniturelogs;

CREATE TABLE loja_web.omniturelogs(
col_1 STRING,col_2 STRING,col_3 STRING,col_4 STRING,col_5 STRING,
col_6 STRING,col_7 STRING,col_8 STRING,col_9 STRING,col_10 STRING,
col_11 STRING,col_12 STRING,col_13 STRING,col_14 STRING,col_15 STRING,
col_16 STRING,col_17 STRING,col_18 STRING,col_19 STRING,col_20 STRING,
col_21 STRING,col_22 STRING,col_23 STRING,col_24 STRING,col_25 STRING,
col_26 STRING,col_27 STRING,col_28 STRING,col_29 STRING,col_30 STRING,
col_31 STRING,col_32 STRING,col_33 STRING,col_34 STRING,col_35 STRING,
col_36 STRING,col_37 STRING,col_38 STRING,col_39 STRING,col_40 STRING,
col_41 STRING,col_42 STRING,col_43 STRING,col_44 STRING,col_45 STRING,
col_46 STRING,col_47 STRING,col_48 STRING,col_49 STRING,col_50 STRING,
col_51 STRING,col_52 STRING,col_53 STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS PARQUET
TBLPROPERTIES("parquet.compression"="SNAPPY");



------------------------------------------------------------
-- INSERINDO DADOS NAS TABELAS
------------------------------------------------------------
INSERT INTO produtos SELECT url, categoria FROM ex_produtos;
INSERT INTO usuarios SELECT * FROM ex_usuarios;
INSERT INTO omniturelogs SELECT * FROM ex_omniture;



------------------------------------------------------------
-- DROP TABELAS EXTERNAS
------------------------------------------------------------
DROP TABLE ex_produtos;
DROP TABLE ex_usuarios;
DROP TABLE ex_omniture;


------------------------------------------------------------
-- CRIANDO TABELA VIEW OMNITURE
------------------------------------------------------------
DROP VIEW IF EXISTS loja_web.omniture;

CREATE VIEW loja_web.omniture AS
SELECT col_2 horario, col_8 ip, col_13 url, col_14 swid, col_50 cidade,
col_51 pais, col_53 estado
FROM loja_web.omniturelogs;

