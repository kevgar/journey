
## Setting up a Hive database on an Amazon EMR Cluster

### (1) Setup the development environmnet

First we manually download the data from [here](https://www.kaggle.com/frtgnn/dunnhumby-the-complete-journey). The download consists of 8 csv files in a folder, which we save as a zip archive to make uploading to/downloading from S3 much faster.

We use an AWS Cloud9 environment to perform the following steps. A suitable enviromnent can be created from your AWS account by clicking on **Services** > **Cloud9** > **Create Environment**, then selecting the default configurations. Once the environment is created click to open the IDE. Using the IDE, you can upload the zip folder into the home directory by clicking on **File** > **Upload Local Files** and selecting the file.

### (2) Create an S3 bucket to stage the data

Next we create an S3 bucket and copy the data into it from our Cloud9 environment. We also create a logs folder for storing outputs.
aws s3 api create-bucket --bucket kjgardner74-hive --region us-east-1
aws s3 cp 408408-782411-bundle-archive.zip s3://kjgardner74-hive/408408-782411-bundle-archive.zip
aws s3 api put-object --bucket kjgardner74-hive --key logs/
### (3) Clone repo and sync scripts to s3 bucket

We also create a folder in our S3 bucket for scripts. This will be useful later if we want to execute SQL scripts on the master node of our EMR cluster, since the cluster will be connected to the S3 bucket.

To make scripts available in S3, we clone this repo in our Cloud9 environment and sync the relevant directory to S3. Note that before cloning into the repo we must generate an SSH key in the Cloud9 shell using the command `ssh-keygen -t rsa` and upload the SSH public key into Github.
git clone git@github.com:kevgar/journey.git
aws s3 sync ~/journey/hive-emr-setup s3://kjgardner74-hive/scripts
### (4) Launch an EMR Cluster

For a database solution we use Hive running on an EMR cluster. A suitable cluster can be created from your AWS account by clicking on **Services** > **EMR** > **Create Cluster**, then selecting the default configurations.

Note we must specify (1) an S3 bucket to link to the cluster and (2) an EC2 key pair for accessing accessing the cluster via SSH. After creating the cluster we can click on **AWS CLI Export** to generate a reusable aws command.
aws emr create-cluster --auto-scaling-role EMR_AutoScaling_DefaultRole --termination-protected --applications Name=Hadoop Naaws emr create-cluster --applications Name=Hadoop Name=Hive Name=Pig Name=Hue Name=Spark --ec2-attributes '{"KeyName":"ec2-journey","InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-eaebacc7","EmrManagedSlaveSecurityGroup":"sg-3e3cba41","EmrManagedMasterSecurityGroup":"sg-eb3bbd94"}' --service-role EMR_DefaultRole --enable-debugging --release-label emr-5.30.1 --log-uri 's3n://kjgardner74-hive/' --name 'emr-journey' --instance-groups '[{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"m3.xlarge","Name":"Master Instance Group"}]' --configurations '[{"Classification":"spark","Properties":{}}]' --scale-down-behavior TERMINATE_AT_TASK_COMPLETION --region us-east-1
### (5) Ssh into the master node

Next we login to the master node of the EMR cluster by importing the EC2 key pair .pem file into our Cloud9 environment (by clicking **File** > **Upload Local Files** in the IDE) then running the following command
aws emr ssh --cluster-id j-1CZ51HG5J04RV --key-pair-file ~/ec2-journey.pem
### (6) Copy the data from S3 and unzip

Once logged in to the master node we copy and unzip the data from the S3 bucket.
aws s3 cp s3://kjgardner74-hive/journey/408408-782411-bundle-archive.zip 408408-782411-bundle-archive.zip
unzip 408408-782411-bundle-archive.zip
mv 408408-782411-bundle-archive journey
### (7) Clean up the home directory
rm -rf 408408-782411-bundle-archive.zip
rm -rf __MACOSX/
### (8) For each desired table, create an HDFS and directory and copy data into it
hdfs dfs -mkdir journey
hdfs dfs -mkdir journey/transaction
hdfs dfs -copyFromLocal journey/transaction_data.csv journey/transaction
hdfs dfs -mkdir journey/product
hdfs dfs -copyFromLocal journey/product.csv journey/product
hdfs dfs -mkdir journey/hh_demographic
hdfs dfs -copyFromLocal journey/hh_demographic.csv journey/hh_demographic
hdfs dfs -mkdir journey/causal
hdfs dfs -copyFromLocal journey/causal_data.csv journey/causal
hdfs dfs -mkdir journey/coupon
hdfs dfs -copyFromLocal journey/coupon.csv journey/coupon
hdfs dfs -mkdir journey/coupon_redempt
hdfs dfs -copyFromLocal journey/coupon_redempt.csv journey/coupon_redempt
hdfs dfs -mkdir journey/campaign
hdfs dfs -copyFromLocal journey/campaign_table.csv journey/campaign
hdfs dfs -mkdir journey/campaign_desc
hdfs dfs -copyFromLocal journey/campaign_desc.csv journey/campaign_desc
### (9) Create Hive tables and load data

Next we create a Hive database then for each table create a schema and load data from the relevant csv file. For this step we can either copy the script `create_hive_tables.hql` from S3 and execute it in batch mode or run the relevant SQL commands interactively. Both options are shown bellow.
aws s3 cp s3://kjgardner74-hive/scripts/create_hive_tables.hql create_hive_tables.hql
hive -hiveconf DATA_DIR=/home/journey/ -f create_hive_tables.hql-- File: create_hive_tables.hql

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
### (10) Clean up

Finally, we delete our S3 bucket contents and Terminate the EMR cluster as follows.

Delete data from S3 bucket.
aws s3 rm s3://kjgardner74-hive --recursive
Terminate EMR Cluster.
aws emr modify-cluster-attributes --cluster-id j-1CZ51HG5J04RV --no-termination-protected
aws emr terminate-clusters --cluster-ids j-1CZ51HG5J04RV