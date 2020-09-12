-- File: create_hive_tables.hql

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