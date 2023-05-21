/* If you are using Notepad++ editor then set language to SQL*/

/*STEP1 : CREATE THE DATABSE objects */
--create new database and schema
CREATE OR REPLACE DATABASE MY_DB;
CREATE OR REPLACE SCHEMA MY_DB.PRODUCTS;

--sequences
create or replace sequence AUTO_SEQ start with 1 increment by 1;
create or replace sequence SUPPLIER_SEQ start with 1 increment by 1;

--Table creation
create or replace TABLE MY_DB.PRODUCTS.AUTO1 (
	NAME VARCHAR(65535),
	UNITS VARCHAR(65535),
	SCALE NUMBER(38,0),
	FREQUENCY VARCHAR(2),
	DATE DATE,
	VALUE FLOAT,
	KEY_ID NUMBER(38,0) DEFAULT MY_DB.PRODUCTS.AUTO_SEQ.NEXTVAL
);


create or replace TABLE MY_DB.PRODUCTS.GEOINFO (
	PARENT_GEO_ID VARCHAR(16777216) COMMENT 'GEO_ID from GEOGRAPHY_INDEX table for the parent geography',
	GEO_ID VARCHAR(16777216) COMMENT 'GEO_ID from GEOGRAPHY_INDEX table for the child geography'
);


create or replace TABLE MY_DB.PRODUCTS.SUPPLIER (
	SUPPLIER_COUNTRY VARCHAR(255),
	SUPPLIER_DOMAIN VARCHAR(255),
	SUPPLIER_NAME VARCHAR(255),
	CUSTOMER_COUNTRY VARCHAR(255),
	CUSTOMER_DOMAIN VARCHAR(255),
	CUSTOMER_NAME VARCHAR(255),
	CUSTOMER_PERMID VARCHAR(255),
	SUPPLIER_KEY NUMBER(38,0) DEFAULT MY_DB.PRODUCTS.SUPPLIER_SEQ.NEXTVAL
);

/*STEP2 : LOAD THE GIVEN CSV files into respective tables  */
--Then load 3 tables which are given in csv format using snowflake UI

/*STEP3 : DEPLOY THE PROCEDURE  */
create or replace procedure MY_DB.PRODUCTS.ENABLE_CHANGE_TRACK_ON_SHARE(SHARE_DB_NAME VARCHAR,SHARE_SCHEMA VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
var stmt=`SELECT 'ALTER VIEW '||TABLE_CATALOG||'.'||TABLE_SCHEMA||'.'||TABLE_NAME||' SET CHANGE_TRACKING=TRUE ;' FROM MY_DB.INFORMATION_SCHEMA.VIEWS WHERE TABLE_CATALOG IN (?) AND TABLE_SCHEMA IN(?);`
var snow_exe=snowflake.createStatement({sqlText:stmt,binds:[SHARE_DB_NAME,SHARE_SCHEMA]});
var snow_exe_stmt=snow_exe.execute();
var count=0;
while(snow_exe_stmt.next())
{    
    snowflake.execute({sqlText:snow_exe_stmt.getColumnValue(1)});
    count=count+1;
}
return count+" object's change_tracking mode is enabled"

$$
;

/*STEP4 : CREATE A SHARE ON THE SECURE VIEW OBJECTS USING BELOW SQL */
CREATE SHARE "MY_DB_SNOWFLAKE_SECURE_SHARE" COMMENT='Creating secure share of business views';
GRANT USAGE ON DATABASE "MY_DB" TO SHARE "MY_DB_SNOWFLAKE_SECURE_SHARE";
GRANT USAGE ON SCHEMA "MY_DB"."PRODUCTS" TO SHARE "MY_DB_SNOWFLAKE_SECURE_SHARE";
GRANT SELECT ON VIEW "MY_DB"."PRODUCTS"."V_AUTOINFO" TO SHARE "MY_DB_SNOWFLAKE_SECURE_SHARE";
GRANT SELECT ON VIEW "MY_DB"."PRODUCTS"."V_GEOINFO" TO SHARE "MY_DB_SNOWFLAKE_SECURE_SHARE";
GRANT SELECT ON VIEW "MY_DB"."PRODUCTS"."V_SUPPLIER" TO SHARE "MY_DB_SNOWFLAKE_SECURE_SHARE";

/*STEP5 : PROVIDE PROPER/CORRECT ACCOUNT ID OF ANOTHER ACCOUNT
Snowflake comes with new feature as one organization can have multiple account so create new one get the account_name using SELECT CURRENT_ACCOUNT()  */
--provide the proper account name
ALTER SHARE "MY_DB_SNOWFLAKE_SECURE_SHARE" ADD ACCOUNTS = xyz; --give the correct account_name

/*test : are you ok to enable change tracking for the result of below sql*/
SELECT 'ALTER VIEW '||TABLE_CATALOG||'.'||TABLE_SCHEMA||'.'||TABLE_NAME||' SET CHANGE_TRACKING=TRUE ;' FROM MY_DB.INFORMATION_SCHEMA.VIEWS
WHERE TABLE_SCHEMA='PRODUCTS' AND TABLE_CATALOG='MY_DB';


/*STEP6 : Execute the below statement to enable change_tracking_mode for shared objects from the schema
Please note here I am sharing all the secured views from schema to enabled change_tracking for all the tables */
CALL MY_DB.PRODUCTS.ENABLE_CHANGE_TRACK_ON_SHARE('MY_DB','PRODUCTS');  --Here MY_DB is database and PRODUCTS is schema of the database shared

---==================================================================================================================================================
--                                    ***** YOU ARE ALL SET WITH PRIMARY ACCOUNT. Lets Move to seconday account
---=========================================================================================================================









