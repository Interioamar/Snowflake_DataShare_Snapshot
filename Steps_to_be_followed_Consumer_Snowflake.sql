---==================================================================================================================================================
--                                    ***** Start here only when all  Source Snowflake Account tasks are completed
---=========================================================================================================================

/*STEP8 : Switch to another snowflake account and see the private sharing inbound data share where we can observe the data shared by primary ACCOUNT
If you can see then create a database on the shared object through Snowflake UI
If you using Snowsight: Click on Download like symbol from the inbound SHARE
Classic Console: Click on inbound datashare and create database
Also provide the roles to which want to give access

Once all the above steps followed You will be able to see the New Inbound datashare in  Seconday Account
*/


/*STEP	9 : Create separate new database to store the procedure and tables to store which are useful for tracking and updating the datashare object relations
*/
CREATE OR REPLACE DATABASE SNAPSHOT;
CREATE OR REPLACE SCHEMA AUTOMATION;

/*STEP10 : Deploy the below procedure which copies the data from databashare objects to new tables at current timestamp
*/
create or replace procedure snapshot.automation.DATASHARE_SNAPSHOT(TARGET_TBL_DB varchar,TARGET_TBL_SCHEMA varchar
                                                                  ,SHARE_DB_NAME varchar,share_schema_name varchar)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
var schema=TARGET_TBL_DB.concat(".",TARGET_TBL_SCHEMA);
var sql_share_db="create database if not exists "+TARGET_TBL_DB+";";
var sql_share_schema="create SCHEMA if not exists "+schema+";";
snowflake.execute({sqlText:sql_share_db});
snowflake.execute({sqlText:sql_share_schema});

var sql_share="use database "+SHARE_DB_NAME+";";
snowflake.execute({sqlText:sql_share});

var sql_query=`select 'SELECT * FROM '||TABLE_CATALOG||'.'||TABLE_SCHEMA||'.'||TABLE_NAME||';' as view_select
,'CREATE OR REPLACE TABLE '||:1||'.'||:2||'.T_'||substr(TABLE_NAME,3)
||' LIKE '||TABLE_CATALOG||'.'||TABLE_SCHEMA||'.'||TABLE_NAME||';' as create_tbl
,'INSERT INTO '||:1||'.'||:2||'.T_'||substr(TABLE_NAME,3)||' '||view_select||';'
from INFORMATION_SCHEMA.VIEWS where TABLE_SCHEMA in (:3);`
var snow_exe1=snowflake.createStatement({sqlText:sql_query,binds:[TARGET_TBL_DB,TARGET_TBL_SCHEMA,SHARE_SCHEMA_NAME]});
var snow_exe=snow_exe1.execute();
var count=0;
try{
while(snow_exe.next())
{
snowflake.execute({sqlText:snow_exe.getColumnValue(1)});
snowflake.execute({sqlText:snow_exe.getColumnValue(2)});
snowflake.execute({sqlText:snow_exe.getColumnValue(3)});

count=count+1;
}

var stmt_stream=`select 'CREATE OR REPLACE STREAM '||:1||'.'||:2||'.'||TABLE_NAME||'_STREAM ON VIEW   '||TABLE_CATALOG||'.'||TABLE_SCHEMA||'.'||TABLE_NAME
from INFORMATION_SCHEMA.VIEWS where TABLE_SCHEMA IN (:3);`;

var snow_exe2=snowflake.createStatement({sqlText:stmt_stream,binds:[TARGET_TBL_DB,TARGET_TBL_SCHEMA,SHARE_SCHEMA_NAME]});
var snow_exe_all=snow_exe2.execute();
var stream_count=0
 while(snow_exe_all.next())
    {
    snowflake.execute({sqlText:snow_exe_all.getColumnValue(1)});
    stream_count=stream_count+1;
    }

result= "Total of "+count+" views or tables copied into new database" + " also "+stream_count+" streams are created on the shared objects"
}
catch(err)
{
 result =  "Failed: Code: " + err.code + "\n  State: " + err.state;
      result += "\n  Message: " + err.message;
      result += "\nStack Trace:\n" + err.stackTraceTxt;

}
return result
$$
;


/*STEP11 : CALL the below procedure to FIRST SNAPSHOT of data from DATASHARE at procedure called timestamp
*/
CALL  snapshot.automation.DATASHARE_SNAPSHOT('SNAPSHOT_STREAMING','PRODUCTS_STREAMING','GLOBAL_SHARE_READ','PRODUCTS');

/*Now whatever the data present at datashare the same number of object and records present in new databse objects
So even if the datashare is removed still our database will have the procedure called timestamp data
*/
---=====================================================================================================================================
  --**    WHAT IF MY DATAHSHARE OBJECTS ARE UPDATING THE RECORDS EVERY 60 MIN and YOU NEED TO HAVE/COPY THAT DATA INTO YOUR NEW DATABASE
  --LIKE SNOWPIPE STREAMING. No No Snowpipe ca not be applied here So here comes the stream and task features. LETS GETS IN DIVE
--===========================================================================================================================


/*STEP12 : Create below tables which are useful for tracking and updating the datashare object relations

AUTOMATION.STREAM_RECORD_STATUS : Is the Status table/audit table which captures how many records got updated/deleted and inserted from datashare into table
*/


create or replace TABLE SNAPSHOT.AUTOMATION.LOOKUP_MAP (
	SOURCE_DB_NAME VARCHAR(16777216),
	TGT_DB_NAME VARCHAR(16777216),
	SOURCE_SCHEMA VARCHAR(16777216),
	TARGET_SCHEMA VARCHAR(16777216),
	SOURCE_TBL VARCHAR(16777216),
	TARGET_TBL VARCHAR(16777216),
	KEY_COL VARCHAR(16777216)
);

create or replace TABLE SNAPSHOT.AUTOMATION.STREAM_RECORD_STATUS (
	ROWS_INSERTED VARCHAR(16777216),
	ROWS_UPDATED VARCHAR(16777216),
	ROWS_DELETED VARCHAR(16777216),
	STREAM_NAME VARCHAR(16777216),
	STREAM_EXECUTED_TIME TIMESTAMP_NTZ(9)
);

/*STEP13 : Load the data into LOOKUP_MAP : It is the mapping table for identifying which stream has to load data into which target table4
and should be manually upload the records based on the number of objects which is necessary
This can also be automated but naming standards will create mess up so did with manually

LOOKUP_MAP_202305201715.csv
*/

/*STEP14 :  Create the below procedure which run the merge statement on each stream if stream has data and loads into target table
*/
create or replace procedure snapshot.automation.stream_merge_stream_has_data(SHARE_DB_NAME VARCHAR,STREAM_DB_NAME VARCHAR,STREAM_SCHEMA VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
//https://snowforce.my.site.com/s/question/0D50Z00008c6KhnSAE/merge-statement-throws-duplicate-error
//https://docs.snowflake.com/en/sql-reference/sql/merge
var deteministic_dup=snowflake.execute({sqlText:`ALTER SESSION SET ERROR_ON_NONDETERMINISTIC_MERGE=false;`});
deteministic_dup.next();

var application_list = `SELECT * from SNAPSHOT.AUTOMATION.LOOKUP_MAP`;
var query_statement = snowflake.createStatement({sqlText: application_list});
var application_list_result = query_statement.execute();

var sql_share="use database "+SHARE_DB_NAME+";";
snowflake.execute({sqlText:sql_share});
var stream_exe_count=0;
var get_debug=[];
 

while(application_list_result.next())
    {   
        
        var KEYCOLUMN= application_list_result.getColumnValue('KEY_COL');
        
        //Check th stream data and decide whether to do merge or not
        var stmt4=`SELECT 'select count(*) from '
                    ||:1||'.'||:2||'.'||SOURCE_TBL||'_STREAM'
                    from SNAPSHOT.AUTOMATION.LOOKUP_MAP where KEY_COL in (:3);`
        var snow_exe4=snowflake.createStatement({sqlText:stmt4,binds:[STREAM_DB_NAME,STREAM_SCHEMA,KEYCOLUMN]});
        var snow_exe_streaming4=snow_exe4.execute();
        snow_exe_streaming4.next();
        var check_count =snowflake.execute({sqlText:snow_exe_streaming4.getColumnValue(1)})
        check_count.next()
        if (check_count.getColumnValue(1)>0) //execute the below statements only if stream has data
        {

        stream_exe_count=stream_exe_count+1;
        //creating joining format
        var join_stmt=`select listagg(join_cond,',') from (SELECT 'a.'||COLUMN_NAME||'=b.'||COLUMN_NAME  as
                    join_cond
                    from INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=:1
                    AND COLUMN_NAME NOT IN (SELECT DISTINCT KEY_COL FROM SNAPSHOT.AUTOMATION.LOOKUP_MAP)
                    and TABLE_NAME =:2);`
        var snow_exe1=snowflake.createStatement({sqlText:join_stmt,binds:[application_list_result.getColumnValue(3),application_list_result.getColumnValue(5)]});
        var snow_exe=snow_exe1.execute();
        snow_exe.next();
        var RESULT_JOIN=snow_exe.getColumnValue(1); //storing the join result in variable
        
        //all column listing from the table
        var stmt2=`select listagg(COLUMN_NAME,',')
                    from INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=:1 and TABLE_NAME =:2;`
        var snow_exe2=snowflake.createStatement({sqlText:stmt2,binds:[application_list_result.getColumnValue(3),application_list_result.getColumnValue(5)]});
        var snow_exe_all=snow_exe2.execute();
        snow_exe_all.next();
        var RESULT_ALL_COL=snow_exe_all.getColumnValue(1); //storing the join result in variable 
        
        
       //implementing merge statement
        var stmt3=`SELECT 'MERGE into '||TGT_DB_NAME||'.'||TARGET_SCHEMA||'.'||TARGET_TBL||' a USING '
                    ||:1||'.'||:2||'.'||SOURCE_TBL||'_STREAM b ON a.'||KEY_COL||' =b.'||KEY_COL
                    ||' WHEN MATCHED AND metadata$action = ''DELETE'''||' AND metadata$isupdate = ''FALSE'''||
                    ' THEN DELETE '||'WHEN MATCHED AND metadata$action = ''INSERT'' AND metadata$isupdate = ''TRUE'''
                    ||' THEN UPDATE SET '||:3||' WHEN NOT MATCHED AND metadata$action = ''INSERT'' AND metadata$isupdate = ''FALSE'''
                    ||' THEN INSERT ('||:4||' ) VALUES ( '||:4||');'
                    from SNAPSHOT.AUTOMATION.LOOKUP_MAP where KEY_COL in (:5);`
        var snow_exe3=snowflake.createStatement({sqlText:stmt3,binds:[STREAM_DB_NAME,STREAM_SCHEMA,RESULT_JOIN,RESULT_ALL_COL,KEYCOLUMN]});
        var snow_exe_streaming=snow_exe3.execute();
        while(snow_exe_streaming.next())
       {
           var exe_merge=snowflake.execute({sqlText:snow_exe_streaming.getColumnValue(1)});
           exe_merge.next();
           
        //audit table filing
        var stream_name = application_list_result.getColumnValue(5)+"_STREAM";
        var audit_tbl=snowflake.createStatement({sqlText:`insert into snapshot.automation.stream_record_status values (?,?,?,?,current_timestamp)`,binds:[exe_merge.getColumnValue(1),exe_merge.getColumnValue(2),exe_merge.getColumnValue(3),stream_name]});
          var snow4=audit_tbl.execute();
           snow4.next(); 
       }
        }

        }
             
return stream_exe_count+" streams got merged to tables "
$$;

/*STEP15 :  CALL the below procedure at first time to see how many streams got consumed if any changes are found datashare objects
*/
CALL snapshot.automation.stream_merge_stream_has_data('GLOBAL_SHARE_READ','SNAPSHOT_STREAMING','PRODUCTS_STREAMING');

---===================================
--  **TIME TO AUTOMATE EVERYTHING AND SIT RELAX  So create a task above procedure for every 60 mins
---===============================

create or replace task TASK_TO_MERGE_STREAMS --3.3PM IST scheduled
  warehouse = COMPUTE_WH2
  schedule = '60 Minutes' 
as
  CALL snapshot.automation.stream_merge_stream_has_data('GLOBAL_SHARE_READ','SNAPSHOT_STREAMING','PRODUCTS_STREAMING');
  
 --Resume the  task as it is by default in suspended case
   alter task task_to_merge_streams resume;


/*STEP16 :  Want to Check the task hesitory and status of run run the below statement
*/

select * from table(information_schema.task_history())
order by scheduled_time desc;

/*STEP17 :  Query on below table to know the status of records executed from datashare to snapshot objects 
*/
select * from SNAPSHOT.AUTOMATION.STREAM_RECORD_STATUS order by STREAM_EXECUTED_TIME desc;


