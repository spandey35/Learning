-- Databricks notebook source
-- MAGIC %md
-- MAGIC # **Database, Schema and Tables**

-- COMMAND ----------

show databases

-- COMMAND ----------

use person

-- COMMAND ----------

show tables

-- COMMAND ----------

select current_database()

-- COMMAND ----------

create database DeltaBase

-- COMMAND ----------

show databases

-- COMMAND ----------

drop database deltabase

-- COMMAND ----------

show databases

-- COMMAND ----------

create schema delatabase

-- COMMAND ----------

show schemas

-- COMMAND ----------

drop schema delatabase

-- COMMAND ----------

create schema delatabase

-- COMMAND ----------

drop schema delatabase cascade

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # **Creating Delta Table**

-- COMMAND ----------

select current_database()

-- COMMAND ----------

drop table if exists t01;
create table t01 (id int, name string)

-- COMMAND ----------

select * from t01;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Inserting Data into delta tables

-- COMMAND ----------

insert into t01 values (1,"suraj1");
insert into t01 values (2,"suraj2");
insert into t01 values (3,"suraj3");

-- COMMAND ----------

insert into t01 values 
(4,"suraj4"),
(5,"suraj5");



-- COMMAND ----------

select * from t01;

-- COMMAND ----------

insert overwrite  t01 values 
(6,"suraj6"),
(7,"suraj7");

-- COMMAND ----------

select * from t01;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **The main difference between Insert into and Insert Overwrite is as follow**
-- MAGIC - Insert into will insert the data into table in appending manner
-- MAGIC - Insert Overwrite will first delete all the data from table and the whatever the values we are inserting that will get insert.
-- MAGIC
-- MAGIC Above the example for same

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # **Basic Utylities**

-- COMMAND ----------

desc t01;

-- COMMAND ----------

describe t01;

-- COMMAND ----------

describe formatted t01;

-- COMMAND ----------

describe extended t01;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC In discribe extended will get one more additional rows with the information what's the size of the statistics of the table but at in the above exmaple its has given same information.  

-- COMMAND ----------

desc detail t01;

-- COMMAND ----------

desc history t01;

-- COMMAND ----------


