# Loja de Varejo na Web
 
## Descrição
Veremos como carregar e consultar dados para uma loja fictícia de varejo na web derivando insights de grandes fontes de dados, como logs da web. Combinando web logs com dados de clientes mais tradicionais, podemos entender melhor os clientes e entender como otimizar futuras promoções e anúncios. Será descrito como inseri dados do HDFS, criar tabelas e consultas a partir do Hive.

## Início
Criaremos alguns diretórios no HDFS onde ficarão os aquivos que serivirão para montar o nosso banco de dados.

```Bash
hdfs dfs -mkdir /tmp/loja_web/{omniture-logs, products, usuarios}

hdfs dfs -put omniture-logs.tsv /tmp/loja_web/omniture-logs
hdfs dfs -put products.tsv /tmp/loja_web/products
hdfs dfs -put usuarios.tsv /tmp/loja_web/usuarios
```

Com os arquivos carregados e prontos para uso, vamos comecar a criar nosso database. Iniciamos criando o database loja_web onde ficará todas as tabelas.
```SQL
DROP DATABASE IF EXISTS loja_web;
CREATE DATABASE loja_web;
```


Em seguida, vamos criar tabelas externas para todos os arquivos que carregamos no HDFS (omniture-logs.tsv, products.tsv, usuarios.tsv).

**usuarios**
```SQL
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
```

**produtos**
```SQL
DROP TABLE IF EXISTS loja_web.ex_produtos;

CREATE EXTERNAL TABLE loja_web.ex_produtos(
url STRING,
categoria STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/tmp/loja_varejo/products'
TBLPROPERTIES("skip.header.line.count"="1");
```

**omniturelogs**
```SQL
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
```

O próximo passo será criar as tabelas gerenciadas:

```SQL
------------------------------------------------------------
-- CRIANDO TABELA USUARIOS
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
```

E vamos inserir os dados das tabelas externas para as tabelas gerenciadas. Essa etapa será um pouco demorada pois temos muitos dados de logs à serem carregados.

```SQL
INSERT INTO produtos SELECT url, categoria FROM ex_produtos;
INSERT INTO usuarios SELECT * FROM ex_usuarios;
INSERT INTO omniturelogs SELECT * FROM ex_omniture;
```

Não precisamos mais das tabelas externas, então vamos executar um drop nelas.

```SQL
DROP TABLE ex_produtos;
DROP TABLE ex_usuarios;
DROP TABLE ex_omniture;
```

A tabela de omniturelogs possue muitas conlunas, vamos ver se podemos pegar somente as colunas que fazem sentido para nosso trabalho. 
```SQL
!set outputformat vertical
SELECT * FROM omniturelogs LIMIT 1
```

![omniturelogs](https://github.com/BrunoHarlis/loja_de_varejo_na_web/blob/main/imagens/omniturelogs.png)

Vamos criar uma view, pegar apenas as colunas que são relevantes e renomea-las para facilitar consultas futuras.
```
COLUNAS SELECIONADAS: col_2: horario
                      col_8: ip
                      col_13: url
                      col_14: swid
                      col_50: cidade
                      col_51: pais
                      col_53: estado
```
```SQL
DROP VIEW IF EXISTS loja_web.omniture;

CREATE VIEW loja_web.omniture AS
SELECT col_2 horario, col_8 ip, col_13 url, col_14 swid, col_50 cidade,
col_51 pais, col_53 estado
FROM loja_web.omniturelogs;
```

![omniture_view](https://github.com/BrunoHarlis/loja_de_varejo_na_web/blob/main/imagens/view_omniture.png)


