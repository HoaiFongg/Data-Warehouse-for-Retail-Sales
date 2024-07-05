use master
CREATE database [DataWarehouse-retail-sales]
use [DataWarehouse-retail-sales]

CREATE TABLE product (
    PRODUCT_ID INT PRIMARY KEY,
    MANUFACTURER INT,
    DEPARTMENT TEXT,
    BRAND TEXT,
    COMMODITY_DESC TEXT,
    SUB_COMMODITY_DESC TEXT,
    CURR_SIZE_OF_PRODUCT TEXT
);

CREATE TABLE transaction_data (
    household_key INT,
    BASKET_ID BIGINT,
    DAY INT,
    PRODUCT_ID INT,
    QUANTITY INT,
    SALES_VALUE DOUBLE,
    STORE_ID INT,
    RETAIL_DISC DOUBLE,
    TRANS_TIME INT,
    WEEK_NO INT,
    COUPON_DISC INT,
    COUPON_MATCH_DISC INT,
    DAY2 TEXT,
    PRIMARY KEY (BASKET_ID, DAY, STORE_ID)
);

CREATE TABLE hh_demographic (
    AGE_DESC TEXT,
    MARITAL_STATUS_CODE TEXT,
    INCOME_DESC TEXT,
    HOMEOWNER_DESC TEXT,
    HH_COMP_DESC TEXT,
    HOUSEHOLD_SIZE_DESC INT,
    KID_CATEGORY_DESC TEXT,
    household_key INT PRIMARY KEY
);

CREATE TABLE campaign_table (
    DESCRIPTION TEXT,
    household_key INT,
    CAMPAIGN INT,
);

CREATE TABLE coupon (
    COUPON_UPC BIGINT PRIMARY KEY,
    PRODUCT_ID INT,
    CAMPAIGN INT
);

CREATE TABLE campaign_desc (
    DESCRIPTION TEXT,
    CAMPAIGN INT PRIMARY KEY,
    START_DAY INT,
    END_DAY INT
);

CREATE TABLE coupon_redempt (
    household_key INT,
    DAY INT,
    COUPON_UPC BIGINT,
    CAMPAIGN INT,
    PRIMARY KEY (DAY, COUPON_UPC)
);

-- Adding Foreign Key Constraints

--Transaction_data table
ALTER TABLE transaction_data
ADD CONSTRAINT FK_transaction_data_hh_demographic
FOREIGN KEY (household_key) REFERENCES hh_demographic(household_key);

ALTER TABLE transaction_data
ADD CONSTRAINT FK_transaction_data_product
FOREIGN KEY (PRODUCT_ID) REFERENCES product(PRODUCT_ID);

--coupon table
ALTER TABLE coupon
ADD CONSTRAINT FK_coupon_product
FOREIGN KEY (PRODUCT_ID) REFERENCES product(PRODUCT_ID);

ALTER TABLE coupon
ADD CONSTRAINT FK_coupon_campaign_desc
FOREIGN KEY (CAMPAIGN) REFERENCES campaign_desc(CAMPAIGN);

--campaign_table table
ALTER TABLE campaign_table
ADD CONSTRAINT FK_campaign_table_hh_demographic
FOREIGN KEY (household_key) REFERENCES hh_demographic(household_key);

ALTER TABLE campaign_table
ADD CONSTRAINT FK_campaign_table_campaign_desc
FOREIGN KEY (CAMPAIGN) REFERENCES campaign_desc(CAMPAIGN);

--coupon_redempt table
ALTER TABLE coupon_redempt
ADD CONSTRAINT FK_coupon_redempt_hh_demographic
FOREIGN KEY (household_key) REFERENCES hh_demographic(household_key);

ALTER TABLE coupon_redempt
ADD CONSTRAINT FK_coupon_redempt_coupon
FOREIGN KEY (CAMPAIGN) REFERENCES campaign_desc(CAMPAIGN);