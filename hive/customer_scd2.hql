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


-- copy into temporary table all records in target that are not in source
insert into customer_temp
select * from customer_temp where customerid not in (select customerid from store.customer);


-- copy into temporary table all records that are in source but not in temporary table and set defalts
insert into customer_temp
select T.customerid, T.accountnumber, T.customertype, T.namestyle, T.title, T.firstname, T.middlename,
T.lastname, T.suffix, T.emailaddress, T.emailpromotion, T.phone, T.additionalcontactinfo, T.territoryid,
T.territoryname,T.countryregioncode, T.`group`, T.modifieddate  , current_date(), true
from customer T
where customerid not in (select customerid from customer_temp);


-- copy into temporary table all records in both stable but match
insert into customer_temp
select a.* from customer_temp a join customer b on a.customerid = b.customerid
where a.`group` = b.`group` and
a.accountnumber = b.accountnumber and
a.customertype = b.customertype and
a.firstname = b.firstname and
a.middlename = b.middlename and
a.emailaddress = b.emailaddress and
a.phone = b.phone and
a.territoryid = b.territoryid and
a.countryregioncode = b.countryregioncode;



-- copy into temporary table all records in both stable but dont match from the target
-- make sure to set end_date and active records to mark their time in history
insert into customer_temp
select b.customerid, b.accountnumber, b.customertype, b.namestyle, b.title, b.firstname, b.middlename,
b.lastname, b.suffix, b.emailaddress, b.emailpromotion, b.phone, b.additionalcontactinfo, b.territoryid,
b.territoryname,b.countryregioncode, b.`group`, current_date(), null,
true from customer_temp a join customer b on a.customerid = b.customerid
where a.`group` = b.`group` or
    a.accountnumber = b.accountnumber or
    a.customertype = b.customertype or
    a.firstname = b.firstname or
    a.middlename = b.middlename or
    a.emailaddress = b.emailaddress or
    a.phone = b.phone or
    a.territoryid = b.territoryid or
    a.countryregioncode = b.countryregioncode;


-- copy into temporary table all records in both stable but dont match from the source
-- make sure to set start_date and active records to mark them as current versions
insert into customer_temp
select b.customerid, b.accountnumber, b.customertype, b.namestyle, b.title, b.firstname, b.middlename,
b.lastname, b.suffix, b.emailaddress, b.emailpromotion, b.phone, b.additionalcontactinfo, b.territoryid,
b.territoryname,b.countryregioncode, b.`group`, current_date(), null,
true from customer a join customer_temp b on a.customerid = b.customerid
where  a.`group` = b.`group` or
a.accountnumber = b.accountnumber or
a.customertype = b.customertype or
a.firstname = b.firstname or
a.middlename = b.middlename or
a.emailaddress = b.emailaddress or
a.phone = b.phone or
a.territoryid = b.territoryid or
a.countryregioncode = b.countryregioncode;

-- insert overwrite from temporary table to target
insert overwrite table store.customer
select T.* from customer_temp as T;


-- drop the temporary table table
drop table customer_temp;