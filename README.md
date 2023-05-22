<p align="center">
<img src=https://github.com/Interioamar/Snowflake_data_snapshot/assets/107593984/876389ef-b44a-48f5-a9fc-a481504c51ac/>
</p>

## **Creating snapshot for DataShare objects and streaming the changed data from Data Share to snapshot objects**

**Objective:**

Snowflake provide datashare feature which enables the read/view access
to the consumer snowflake account when it is shared to another snowflake
account within or outside organization.

Consider a scenario where user what to do testing with source objects
and target objects where target objects are developed in another account
of Snowflake . So user need snapshot (Persistant/Physicallized) data
from inbound datashare into new database which is persistant so that
even the data share is been revoked/removed from the source snowflake
still consumer snowflake account will have snapshot data at particular
timestamp by which testing/data pipeline can be smoothly handled.

In some cases consumer snowflake account wanted to continuosly monitor
the data changes happening in the source snowflake datashare and same
neds to update in the consumer snowflake snapshot database objects(Like
streaming the updated data from datashare using delete,insert and update
DML operations)

**Why this case study:**

-   Snowflake does not allow to do zero copy cloning on datashare
    objects and is read only.

-   Data snapshot can not be acheived on inbound datashare objects

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

**Understanding used database and other details**

Source: Snowflake(Data Provider) SF1

Target : Snowflake(Consumer) SF2

Language Used: SQL, JavaScript

### **Flow Daigram/Approach design**
![image2](https://github.com/Interioamar/Snowflake_data_snapshot/assets/107593984/c6cb5496-da13-4794-a322-d0c74fee2e4d)



