use store_stage;

drop table if exists customer_temp;
-- create a temporary table
create temporary table customer_temp (
    customerid int,
    accountnumber  string,
    customertype varchar(1),
    namestyle boolean,
    title string,
    firstname string ,
    middlename  string,
    lastname string ,
    suffix string ,
    emailaddress  string,
    emailpromotion int,
    phone  string,
    additionalcontactinfo  string,
    territoryid int,
    territoryname  string,
    countryregioncode  string,
    `group`  string,
    modifieddate bigint,
    change_date timestamp,
    active boolean
)
stored as parquet;


-- copy into temporary table all records in target(store) that are not in source(store_stage)
insert into customer_temp
select * from store.customer where customerid not in (select customerid from customer);

-- copy into temporary table all records in source(store_stage) that are not in target(store)
insert into customer_temp
select T.customerid, T.accountnumber, T.customertype, T.namestyle, T.title, T.firstname, T.middlename,
T.lastname, T.suffix, T.emailaddress, T.emailpromotion, T.phone, T.additionalcontactinfo, T.territoryid,
T.territoryname,T.countryregioncode, T.`group`, T.ModifiedDate, current_date(), true
from customer T where T.customerid not in (select customerid from store.customer);

-- copy into temporary table all records that are updated from source(store_stage) which are in target(store_stage) set the flag value as true
insert into customer_temp
select T.customerid, T.accountnumber, T.customertype, T.namestyle, T.title, T.firstname, T.middlename,
T.lastname, T.suffix, T.emailaddress, T.emailpromotion, T.phone, T.additionalcontactinfo, T.territoryid,
T.territoryname,T.countryregioncode, T.`group`, T.ModifiedDate, current_date(), true
from customer T
join store.customer a on T.customerid = a.customerid
where a.accountnumber != T.accountnumber or
a.customertype != T.customertype or
a.firstname != T.firstname or
a.middlename != T.middlename or
a.emailaddress != T.emailaddress or
a.phone != T.phone or
a.territoryid != T.territoryid or
a.countryregioncode != T.countryregioncode;

-- copy into temporary table all records that are not updated from source(store_stage) which are in target(store_stage) set the flag value as true
insert into customer_temp
select T.customerid, T.accountnumber, T.customertype, T.namestyle, T.title, T.firstname, T.middlename,
T.lastname, T.suffix, T.emailaddress, T.emailpromotion, T.phone, T.additionalcontactinfo, T.territoryid,
T.territoryname,T.countryregioncode, T.`group`, T.ModifiedDate, current_date(), true
from customer T
join store.customer a on T.customerid = a.customerid
where a.accountnumber = T.accountnumber and
a.customertype = T.customertype and
a.firstname = T.firstname and
a.middlename = T.middlename and
a.emailaddress = T.emailaddress and
a.phone = T.phone and
a.territoryid = T.territoryid and
a.countryregioncode = T.countryregioncode;

-- copy into temporary table all records from target(store) which are updated in source(store_stage) set the flag value as false
insert into customer_temp
select T.customerid, T.accountnumber, T.customertype, T.namestyle, T.title, T.firstname, T.middlename,
T.lastname, T.suffix, T.emailaddress, T.emailpromotion, T.phone, T.additionalcontactinfo, T.territoryid,
T.territoryname,T.countryregioncode, T.`group`, T.ModifiedDate, current_date(), false
from store.customer T
join customer a on T.customerid = a.customerid
where T.customerid  in (select customerid from customer) and
(a.accountnumber != T.accountnumber or
a.customertype != T.customertype or
a.firstname != T.firstname or
a.middlename != T.middlename or
a.emailaddress != T.emailaddress or
a.phone != T.phone or
a.territoryid != T.territoryid or
a.countryregioncode != T.countryregioncode);


-- insert overwrite from temporary table to target
insert overwrite table store.customer
select * from customer_temp;


-- drop the temporary table table
drop table customer_temp;