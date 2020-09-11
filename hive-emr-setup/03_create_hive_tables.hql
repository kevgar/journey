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
HOUSEHOLD_KEY BIGINT )
COMMENT 'Household Demographics in Journey'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
tblproperties ("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH 'hh_demographic.csv' OVERWRITE INTO TABLE hh_demographic;

/* --------------------
load transaction_data.csv
-------------------- */

CREATE TABLE transaction_data
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

LOAD DATA LOCAL INPATH 'transaction_data.csv' OVERWRITE INTO TABLE transaction_data;

/* --------------------
load campaign_desc.csv
-------------------- */

/* --------------------
load causal_data.csv
-------------------- */

/* --------------------
load coupon_redempt.csv
-------------------- */

/* --------------------
load product.csv
-------------------- */

/* --------------------
load campaign_table.csv
-------------------- */

/* --------------------
load coupon.csv
-------------------- */
