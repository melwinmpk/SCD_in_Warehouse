create database if not exists store;

use store;

DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS customer_demo;
DROP TABLE IF EXISTS customer_demo_history;
DROP TABLE IF EXISTS creditcard;
DROP TABLE IF EXISTS product;


create external table customer (
    CustomerID int,
    AccountNumber  string,
    CustomerType varchar(1),
    NameStyle boolean,
    Title string,
    FirstName string ,
    MiddleName  string,
    LastName string ,
    Suffix string ,
    EmailAddress  string,
    EmailPromotion int,
    Phone  string,
    AdditionalContactInfo  string,
    TerritoryID int,
    territoryName  string,
    countryregioncode  string,
    `group`  string,
    ModifiedDate timestamp,
    change_date timestamp,
    active boolean
)
stored as parquet
location '/user/saif/HFS/Output/stores/target/customers';



create external table customer_demo
(
    customerid int,
    totalpurchaseytd float,
    datefirstpurchase date,
    birthdate date,
    maritalstatus string,
    yearlyincome string,
    gender string,
    totalchildren int,
    numberchildrenathome int,
    education string,
    occupation string,
    homeownerflag string,
    numbercarsowned int,
    commutedistance string,
    create_date date
)
stored as orc
location '/user/saif/HFS/Output/stores/target/customer_demo';


create external table customer_demo_history
(
    customerid int,
    totalpurchaseytd float,
    datefirstpurchase date,
    birthdate date,
    maritalstatus string,
    yearlyincome string,
    gender string,
    totalchildren int,
    numberchildrenathome int,
    education string,
    occupation string,
    homeownerflag string,
    numbercarsowned int,
    commutedistance string,
    create_date date,
    end_date date
)
stored as orc
location '/user/saif/HFS/Output/stores/target/customer_demo_history';

set hive.support.concurrency=true;
set hive.enforce.bucketing=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.compactor.initiator.on=true;
set hive.compactor.worker.threads=1;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=100;
set hive.exec.max.dynamic.partitions.pernode=100;
Set hive.auto.convert.join=false;
Set hive.merge.cardinality.check=false;

create table creditcard (
    creditcardid int,
    cardtype string,
    cardnumber string,
    expmonth int,
    expyear int,
    modifieddate date,
    create_date date
)
stored as orc
location '/user/saif/HFS/Output/stores/target/creditcard'
TBLPROPERTIES ("transactional"="true");

create external table product (
    productId int,
    name string,
    productnumber string,
    makeflag boolean,
    finishedgoodsflag boolean,
    color string,
    safetystocklevel int,
    reorderpoint int,
    standardcost double,
    listprice double,
    size string,
    sizeunitmeasurecode string,
    weightunitmeasurecode string,
    weight string,
    daystomanufacture int,
    productline string,
    class string,
    style string,
    sellstartdate date,
    sellenddate date,
    discontinueddate date,
    productsubcategory string,
    productcategory string,
    producemodel string,
    catalogdescription string,
    instructions string,
    modifieddate date
)
stored as orc
location '/user/saif/HFS/Output/stores/target/products';
