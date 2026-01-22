CREATE OR REPLACE TABLE transactions_staging AS
SELECT * FROM CREDITDEBIT_TRANSACTIONS_FAST_FOOD_AND_QUICK_SERVICE_RESTAURANTS.SNOWFLAKE_MARKETPLACE.QSR_TRANSACTIONS_SAMPLE;

-- Dimenzia Cardholder
CREATE OR REPLACE TABLE dim_cardholder AS
SELECT DISTINCT
    ACCOUNT_ID AS cardholder_id,
    CARD_ID,
    CARD_TYPE,
    CARD_HOLDER_GENERATION,
    CARD_HOLDER_PERSONAS,
    CARD_HOLDER_VINTAGE,
    CARD_HOLDER_CITY,
    CARD_HOLDER_STATE,
    CARD_HOLDER_POSTAL_CODE,
    CARD_HOLDER_POSTAL_CODE_PRESENT,
    CARD_HOLDER_MSA,
    CARD_HOLDER_LATITUDE,
    CARD_HOLDER_LONGITUDE
FROM HIPPO_TRANSACTIONS_DB.PUBLIC.TRANSACTIONS_STAGING;

-- Dimenzia Merchant
CREATE OR REPLACE TABLE dim_merchant AS
SELECT DISTINCT
    MERCHANT_ID AS,
    MERCHANT_NAME,
    MERCHANT_CATEGORY_CODE,
    TOP_MERCHANT_CATEGORY_CODE,
    MERCHANT_CATEGORY_LEVEL_1,
    MERCHANT_CATEGORY_LEVEL_2,
    MERCHANT_CATEGORY_LEVEL_3,
    MERCHANT_STORE_ID,
    MERCHANT_STORE_ADDRESS,
    MERCHANT_STORE_LOCATION,
    MERCHANT_CITY,
    MERCHANT_STATE,
    MERCHANT_POSTAL_CODE,
    MERCHANT_MSA,
    MERCHANT_LATITUDE,
    MERCHANT_LONGITUDE
FROM HIPPO_TRANSACTIONS_DB.PUBLIC.TRANSACTIONS_STAGING;

-- Dimenzia Payment
CREATE OR REPLACE TABLE dim_payment AS
SELECT DISTINCT
    PAYMENT_ID,
    PAYMENT_NAME,
    CARD_TYPE
FROM HIPPO_TRANSACTIONS_DB.PUBLIC.TRANSACTIONS_STAGING;

-- Dimenzia Date, location
CREATE OR REPLACE TABLE dim_transaction AS
SELECT DISTINCT
    TRANSACTION_DATE,
    TRANSACTION_CITY,
    TRANSACTION_STATE,
    TRANSACTION_POSTAL_CODE,
    TRANSACTION_MSA,
    TRANSACTION_LATITUDE,
    TRANSACTION_LONGITUDE,
    TRANSACTION_DESCRIPTION
FROM HIPPO_TRANSACTIONS_DB.PUBLIC.TRANSACTIONS_STAGING;
CREATE OR REPLACE TABLE fact_transactions AS
SELECT
    TRANSACTION_ID AS transaction_id,
    ACCOUNT_ID AS cardholder_id,
    MERCHANT_ID AS merchant_id,
    PAYMENT_ID AS payment_id,
    TRANSACTION_DATE AS transaction_date,
    GROSS_TRANSACTION_AMOUNT,
    CARD_HOLDER_AVERAGE_LTM_SPEND,
    CARD_HOLDER_AVERAGE_LTM_TRANSACTION_COUNT,
    CARD_HOLDER_TOTAL_SPEND,
    CARD_HOLDER_TOTAL_TRANSACTION_COUNT,
    CONSISTENT_SHOPPER,
    CARD_PRESENT_INDICATOR,
    TRANSACTION_TYPE,
    CURRENCY_CODE
FROM HIPPO_TRANSACTIONS_DB.PUBLIC.TRANSACTIONS_STAGING;

-- Dimenzia Date
CREATE OR REPLACE TABLE dim_date AS
SELECT DISTINCT
    CAST(transaction_date AS DATE) AS transaction_date,
    EXTRACT(YEAR FROM transaction_date) AS year,
    EXTRACT(MONTH FROM transaction_date) AS month,
    EXTRACT(DAY FROM transaction_date) AS day
FROM dim_transaction;

-- Dimenzia Cardholder
CREATE OR REPLACE TABLE dim_cardholder AS
SELECT DISTINCT
    cardholder_id,
    card_holder_generation,
    card_holder_personas,
    card_holder_city,
    card_holder_state,
    card_holder_msa,
    card_holder_postal_code,
    card_holder_postal_code_present,
    card_holder_vintage
FROM dim_cardholder;

-- Dimenzia Merchant
CREATE OR REPLACE TABLE dim_merchant AS
SELECT DISTINCT
    merchant_id,
    merchant_name,
    merchant_category_code,
    merchant_category_level_1,
    merchant_category_level_2,
    merchant_category_level_3,
    merchant_city,
    merchant_state,
    merchant_msa,
    merchant_postal_code
FROM dim_merchant;


CREATE OR REPLACE TABLE dim_payment AS
SELECT DISTINCT
    payment_id,
    payment_name,
    card_type
FROM dim_payment;
CREATE OR REPLACE TABLE fact_transactions AS
SELECT
    transaction_id,
    cardholder_id,
    merchant_id,
    payment_id,
    CAST(transaction_date AS DATE) AS transaction_date,
    gross_transaction_amount,
    card_holder_average_ltm_spend,
    card_holder_average_ltm_transaction_count,
    card_holder_total_spend,
    card_holder_total_transaction_count,
    consistent_shopper,
    card_present_indicator,
    ROW_NUMBER() OVER (
        PARTITION BY cardholder_id
        ORDER BY transaction_date
    ) AS transaction_sequence_number
FROM fact_transactions;

SELECT
    d.year,
    d.month,
    SUM(f.gross_transaction_amount) AS total_revenue
FROM fact_transactions f
JOIN dim_date d ON f.transaction_date = d.transaction_date
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

SELECT
    m.merchant_name,
    SUM(f.gross_transaction_amount) AS total_revenue
FROM fact_transactions f
JOIN dim_merchant m ON f.merchant_id = m.merchant_id
GROUP BY m.merchant_name
ORDER BY total_revenue DESC
LIMIT 10;

SELECT
    p.payment_name,
    AVG(f.gross_transaction_amount) AS avg_transaction_value
FROM fact_transactions f
JOIN dim_payment p ON f.payment_id = p.payment_id
GROUP BY p.payment_name;

SELECT
    c.card_holder_generation,
    SUM(f.gross_transaction_amount) AS total_revenue
FROM fact_transactions f
JOIN dim_cardholder c ON f.cardholder_id = c.cardholder_id
GROUP BY c.card_holder_generation;

SELECT
    transaction_sequence_number,
    COUNT(*) AS transaction_count
FROM fact_transactions
GROUP BY transaction_sequence_number
ORDER BY transaction_sequence_number;
