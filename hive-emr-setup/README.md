
## Loading data into Hive using EMR cluster

## (1) Create Your Own S3 Bucket
$ # Create s3 bucket
$ aws s3api create-bucket --bucket kjgardner74-hive --region us-east-1
$ # Create logs folder
$ aws s3api put-object --bucket kjgardner74-hive --key logs/
$ # Sync local folder to s3 bucket
$ aws s3 sync ~/journey/hive-emr-setup s3://kjgardner74-hive/scripts
## (2) Create an EMR Cluster
$ aws emr create-cluster --auto-scaling-role EMR_AutoScaling_DefaultRole --termination-protected --applications Name=Hadoop Naaws emr create-cluster --applications Name=Hadoop Name=Hive Name=Pig Name=Hue Name=Spark --ec2-attributes '{"KeyName":"ec2-journey","InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-eaebacc7","EmrManagedSlaveSecurityGroup":"sg-3e3cba41","EmrManagedMasterSecurityGroup":"sg-eb3bbd94"}' --service-role EMR_DefaultRole --enable-debugging --release-label emr-5.30.1 --log-uri 's3n://kjgardner74-hive/' --name 'emr-journey' --instance-groups '[{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"m3.xlarge","Name":"Master Instance Group"}]' --configurations '[{"Classification":"spark","Properties":{}}]' --scale-down-behavior TERMINATE_AT_TASK_COMPLETION --region us-east-1
ssh into the cluster
$ aws emr ssh --cluster-id j-1CZ51HG5J04RV --key-pair-file ~/ec2-journey.pem
## (3) Copy the zip file from S3 and unzip
# copy data from s3 over to the master node
$ aws s3 cp s3://kjgardner74-hive/journey/408408-782411-bundle-archive.zip 408408-782411-bundle-archive.zip

# create a directory for the data
$ mkdir -p journey

# unzip the data
$ unzip 408408-782411-bundle-archive.zip

# rename the unzipped directory
$ mv 408408-782411-bundle-archive journey

# clean up the home directory
$ rm -rf 408408-782411-bundle-archive.zip
$ rm -rf __MACOSX/
Copy query scripts from S3
$ aws s3 cp s3://kgardn15-elasticmapreduce/scripts/create_hive_tables.hql create_hive_tables.hql
$ aws s3 cp s3://kgardn15-elasticmapreduce/scripts/create_external_tables.hql create_external_tables.hql
## (4) Create a directory and copy data into hdfs
$ hdfs dfs -mkdir journey
$ 
$ hdfs dfs -mkdir journey/transaction
$ hdfs dfs -copyFromLocal journey/transaction_data.csv journey/transaction
$ 
$ hdfs dfs -mkdir journey/product
$ hdfs dfs -copyFromLocal journey/product.csv journey/product
$ 
$ hdfs dfs -mkdir journey/hh_demographic
$ hdfs dfs -copyFromLocal journey/hh_demographic.csv journey/hh_demographic
$ 
$ hdfs dfs -mkdir journey/causal
$ hdfs dfs -copyFromLocal journey/causal_data.csv journey/causal
$ 
$ hdfs dfs -mkdir journey/coupon
$ hdfs dfs -copyFromLocal journey/coupon.csv journey/coupon
$ 
$ hdfs dfs -mkdir journey/coupon_redempt
$ hdfs dfs -copyFromLocal journey/coupon_redempt.csv journey/coupon_redempt
$ 
$ hdfs dfs -mkdir journey/campaign
$ hdfs dfs -copyFromLocal journey/campaign_table.csv journey/campaign
$ 
$ hdfs dfs -mkdir journey/campaign_desc
$ hdfs dfs -copyFromLocal journey/campaign_desc.csv journey/campaign_desc
## Creating tables in hive

Create a Demographic Table and Load Data into it. Then create a transaction table and load data.
$ hive -hiveconf DATA_DIR=/home/journey/ -f create_hive_tables.hql-- File: create_hive_tables.hql

Create database journey;

Use journey;

/* --------------------
load hh_demographic.csv
-------------------- */

CREATE TABLE hh_demographic
(AGE_DESC VARCHAR(45),
MARITAL_STATUS_CODE VARCHAR(10),
INCOME_DESC VARCHAR(15),
HOMEOWNER_DESC VARCHAR(45),
HH_COMP_DESC VARCHAR(45),
HOUSEHOLD_SIZE_DESC VARCHAR(20),
KID_CATEGORY_DESC VARCHAR(5),
HOUSEHOLD_KEY BIGINT)
COMMENT 'Household Demographics in Journey'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
tblproperties ("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH 'journey/hh_demographic.csv' OVERWRITE INTO TABLE hh_demographic;

/* --------------------
load transaction_data.csv
-------------------- */

CREATE TABLE transaction
(HOUSEHOLD_KEY INT,
BASKET_ID BIGINT,
DAY INT,
PRODUCT_ID BIGINT,
QUANTITY INT,
SALES_VALUE DECIMAL(10,2),
STORE_ID INT,
RETAIL_DISC DECIMAL(5,2),
TRANS_TIME INT,
WEEK_NO INT,
COUPON_DISC DECIMAL(6,2),
COUPON_MATCH_DISC DECIMAL(6,2))
COMMENT 'transactions in Journey'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
tblproperties ("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH 'journey/transaction_data.csv' OVERWRITE INTO TABLE transaction;

/* --------------------
load campaign_desc.csv
-------------------- */

CREATE TABLE campaign_desc
(DESCRIPTION VARCHAR(15),
CAMPAIGN INT,
START_DAY INT,
END_DAY INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
tblproperties ("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH 'journey/campaign_desc.csv' OVERWRITE INTO TABLE campaign_desc;

/* --------------------
load causal_data.csv
-------------------- */

CREATE TABLE causal
(PRODUCT_ID BIGINT,
STORE_ID INT,
WEEK_NO INT,
DISPLAY VARCHAR(5),
MAILER VARCHAR(5))
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
tblproperties ("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH 'journey/causal_data.csv' OVERWRITE INTO TABLE causal;

/* --------------------
load coupon_redempt.csv
-------------------- */

CREATE TABLE coupon_redempt
(HOUSEHOLD_KEY BIGINT,
DAY INT,
COUPON_UPC BIGINT,
CAMPAIGN INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
tblproperties ("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH 'journey/coupon_redempt.csv' OVERWRITE INTO TABLE coupon_redempt;

/* --------------------
load product.csv
-------------------- */

CREATE TABLE product
(PRODUCT_ID INT,
MANUFACTURER INT,
DEPARTMENT STRING,
BRAND STRING,
COMMODITY_DESC STRING,
SUB_COMMODITY_DESC STRING,
CURR_SIZE_OF_PRODUCT STRING)
COMMENT 'journey product list cleaned'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
tblproperties ("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH 'journey/product.csv' OVERWRITE INTO TABLE product;

/* --------------------
load campaign_table.csv
-------------------- */

CREATE TABLE campaign
(DESCRIPTION VARCHAR(15),
HOUSEHOLD_KEY BIGINT,
CAMPAIGN INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
tblproperties ("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH 'journey/campaign_table.csv' OVERWRITE INTO TABLE campaign;

/* --------------------
load coupon.csv
-------------------- */

CREATE TABLE coupon
(COUPON_UPC BIGINT,
PRODUCT_ID INT,
CAMPAIGN INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
tblproperties ("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH 'journey/coupon.csv' OVERWRITE INTO TABLE coupon;
## Now try to create a product table by converting MySQL code to Hive SQL. Use the external table command.
$ hive -hiveconf DATA_DIR=/home/journey/ -f create_external_tables.hql-- File: create_external_tables.hql

USE journey;

/* --------------------
load hh_demographic.csv
-------------------- */

CREATE EXTERNAL TABLE IF NOT EXISTS hh_demographic
(AGE_DESC VARCHAR(45),
MARITAL_STATUS_CODE VARCHAR(10),
INCOME_DESC VARCHAR(15),
HOMEOWNER_DESC VARCHAR(45),
HH_COMP_DESC VARCHAR(45),
HOUSEHOLD_SIZE_DESC VARCHAR(20),
KID_CATEGORY_DESC VARCHAR(5),
HOUSEHOLD_KEY BIGINT)
COMMENT 'Household Demographics in Journey'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
tblproperties ("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH 'journey/hh_demographic.csv' OVERWRITE INTO TABLE hh_demographic;

/* --------------------
load transaction_data.csv
-------------------- */

CREATE EXTERNAL TABLE IF NOT EXISTS transaction
(HOUSEHOLD_KEY INT,
BASKET_ID BIGINT,
DAY INT,
PRODUCT_ID BIGINT,
QUANTITY INT,
SALES_VALUE DECIMAL(10,2),
STORE_ID INT,
RETAIL_DISC DECIMAL(5,2),
TRANS_TIME INT,
WEEK_NO INT,
COUPON_DISC DECIMAL(6,2),
COUPON_MATCH_DISC DECIMAL(6,2))
COMMENT 'transactions in Journey'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
tblproperties ("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH 'journey/transaction_data.csv' OVERWRITE INTO TABLE transaction;

/* --------------------
load campaign_desc.csv
-------------------- */

CREATE EXTERNAL TABLE IF NOT EXISTS campaign_desc
(DESCRIPTION VARCHAR(15),
CAMPAIGN INT,
START_DAY INT,
END_DAY INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
tblproperties ("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH 'journey/campaign_desc.csv' OVERWRITE INTO TABLE campaign_desc;

/* --------------------
load causal_data.csv
-------------------- */

CREATE EXTERNAL TABLE IF NOT EXISTS causal
(PRODUCT_ID BIGINT,
STORE_ID INT,
WEEK_NO INT,
DISPLAY VARCHAR(5),
MAILER VARCHAR(5))
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
tblproperties ("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH 'journey/causal_data.csv' OVERWRITE INTO TABLE causal;

/* --------------------
load coupon_redempt.csv
-------------------- */

CREATE EXTERNAL TABLE IF NOT EXISTS coupon_redempt
(HOUSEHOLD_KEY BIGINT,
DAY INT,
COUPON_UPC BIGINT,
CAMPAIGN INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
tblproperties ("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH 'journey/coupon_redempt.csv' OVERWRITE INTO TABLE coupon_redempt;

/* --------------------
load product.csv
-------------------- */

CREATE EXTERNAL TABLE IF NOT EXISTS product
(PRODUCT_ID INT,
MANUFACTURER INT,
DEPARTMENT STRING,
BRAND STRING,
COMMODITY_DESC STRING,
SUB_COMMODITY_DESC STRING,
CURR_SIZE_OF_PRODUCT STRING)
COMMENT 'journey product list cleaned'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
tblproperties ("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH 'journey/product.csv' OVERWRITE INTO TABLE product;

/* --------------------
load campaign_table.csv
-------------------- */

CREATE EXTERNAL TABLE IF NOT EXISTS campaign
(DESCRIPTION VARCHAR(15),
HOUSEHOLD_KEY BIGINT,
CAMPAIGN INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
tblproperties ("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH 'journey/campaign_table.csv' OVERWRITE INTO TABLE campaign;

/* --------------------
load coupon.csv
-------------------- */

CREATE EXTERNAL TABLE IF NOT EXISTS coupon
(COUPON_UPC BIGINT,
PRODUCT_ID INT,
CAMPAIGN INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
tblproperties ("skip.header.line.count"="1");
## Join Steps

Create a new table by joining transactions and demographics and
keeping only households with kids. See hive slides on page 17 for
help.

$ hive -hiveconf DATA_DIR=/home/JourneyData/ -f createTableFromJoin.hql-- File: createTableFromJoin.hql

use journey;
CREATE TABLE TRANSACTION_DEMOGRAPHIC AS 
select b.household_key, b.basket_id, b.day, b.product_id, b.quantity, 
b.sales_value, b.store_id, b.retail_disc, b.trans_time, b.week_no, 
b.coupon_disc, b.coupon_match_disc, a.age_desc, a.marital_status_code, 
a.income_desc, a.homeowner_desc, a.hh_comp_desc, a.household_size_desc, 
a.kid_category_desc
from TRANSACTION_DATA b JOIN HH_DEMOGRAPHIC a 
ON (a.HOUSEHOLD_KEY=b.HOUSEHOLD_KEY) 
and a.kid_category_desc != 'None/';
Find the top 5 households with children in terms of purchases in dollars. Based on this data, do households with children spend more on average than households without children? Do a print screen to show your results.Create a new table by joining transactions and demographics and keeping only households with kids.

Top 5 households with kids
$ hive -e "select sum(SALES_VALUE) as sum_sales_value from journey.TRANSACTION_DATA a join journey.HH_DEMOGRAPHIC b on (a.household_key=b.household_key) and b.KID_CATEGORY_DESC!='None/' group by b.household_key order by sum_sales_value desc limit 5;"
<img src='image1.png'>

Top 5 households without kids
$ hive -e "select sum(SALES_VALUE) as sum_sales_value from journey.TRANSACTION_DATA a join journey.HH_DEMOGRAPHIC b on (a.household_key=b.household_key) and b.KID_CATEGORY_DESC='None/' group by b.household_key order by sum_sales_value desc limit 5;"
<img src='image2.png'>

## (5) Partitioning

Create a partitioned Transaction_table partitioning by store_id.
$ hive -hiveconf DATA_DIR=/home/JourneyData/ -f partitionedTable.hql-- File: partitionedTable.hql

use journey; 

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=2000;

CREATE TABLE Transaction_table 
(HOUSEHOLD_KEY INT, BASKET_ID BIGINT, DAY INT, PRODUCT_ID BIGINT, QUANTITY INT, SALES_VALUE DECIMAL(10,2), RETAIL_DISC DECIMAL(5,2), TRANS_TIME INT, WEEK_NO INT, COUPON_DISC DECIMAL(6,2), COUPON_MATCH_DISC DECIMAL(6,2))
PARTITIONED BY (STORE_ID INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
tblproperties ("skip.header.line.count"="1");

insert overwrite table transaction_table PARTITION (store_id) 
select household_key, basket_id, day, product_id, quantity, sales_value, 
retail_disc, trans_time, week_no, coupon_disc, coupon_match_disc, store_id     
from transaction_data;
Open a second shell window and check in the hive directories for the partitioned table. Take a screen shot and turn that in.
$ hive -e 'describe journey.transaction_table;'
<img src='image3.png'>

................NEED TO DO................

Find out why there are no seperate directories for the partitioned table.

Querying partitioned table:
$ hive -e 'select count(distinct household_key) as number_households from journey.transaction_table where store_id=310;'
<img src='image4.png'>

Querying full table:
$ hive -e 'select count(distinct household_key) as number_households from journey.transaction_data where store_id=310;'
<img src='image5.png'>

## (5) Create an External Table

Load the product data into HDFS using appropriate commands (already done in step 4). Show how to create an external table on top of this HDFS file.
$ hive -e 'export table journey.product to "journey/product";'
................NEED TO DO................

Explain the value of an external table.

## (6) Run a Batch Job

See above.

## (7) Clean up

Delete data (scripts) from S3 bucket.
$ exit # close ssh connection
$ aws s3 rm ss3://kjgardner74-hive/scripts --recursive
Terminate EMR Cluster.
$ aws emr modify-cluster-attributes --cluster-id j-1CZ51HG5J04RV --no-termination-protected
$ aws emr terminate-clusters --cluster-ids j-1CZ51HG5J04RV