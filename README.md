<p align="center">
<img src=https://github.com/Interioamar/Snowflake_DataShare_Snapshot/assets/107593984/6e3914b3-c97f-4b2b-9048-6f3814615228/>
</p>

## **Creating snapshot for DataShare objects and streaming the changed data from Data Share to snapshot objects**

**Objective:**

Snowflake provide datashare feature which enables the read  access to the objects shared to the consumer 
snowflake account when it is shared to another snowflake
account within or outside organization.

Most of the time, An analyst/user wants to do testing on source objects(Data Share objects from another Snowflake) 
with target objects . So user need snapshot (Persistant/Physicalized) data from inbound datashare into new database 
which is persistant so that even the data share is been revoked/removed from the source snowflake still consumer 
snowflake account will have snapshot data at particular timestamp by which testing/data pipeline can be smoothly handled.

In some cases, consumer snowflake account user/analyst expect to continuosly monitor the data changes
happening in the source snowflake datashare and same needs to be updated in the consumer snowflake 
snapshot database objects(Like streaming the updated data from datashare using delete,insert and update DML operations).
So in this case study we will be implementing the above two cases.

**Challenges in Snowflake:**

-   Snowflake does not allow to do zero copy cloning on datashare
    objects and is read only.

-   Data snapshot can not be acheived directly  on inbound datashare objects

-   Data streaming to get the updated data from inbound datashare into
    snapshot database is not allowed

**Why snowflake didn't consider above points:**

-   It is due to data security guidelines set by snowflake

-   Snowflake says datashare from another account is meant as source or
    master data which should be only used for reading/querying and not
    for copying/manipulations

**Snowflake features used in this projects are:**

-   Automating the sql generaion using information_schema objects

-   Streams

-   Procedures

-   Task

-   Tables and Secure Views

-   Data Share

-   Snowsight Dashboard

**Understanding used database and other details**

Source: Snowflake(Data Provider) SF1

Target : Snowflake(Consumer) SF2

Language Used: SQL, JavaScript

### **Flow Daigram/Approach design**
![image2](https://github.com/Interioamar/Snowflake_DataShare_Snapshot/assets/107593984/33480fb2-f19b-4fe3-a171-b8c899046b7e)

### Merged records status from source snowflake to Consumer Snowflake
![dashboard_stream_merged_data](https://github.com/Interioamar/Snowflake_DataShare_Snapshot/assets/107593984/4f656854-02bc-4360-b74c-8bd6b7188144)



