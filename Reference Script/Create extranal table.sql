CREATE DATABASE SCOPED CREDENTIAL Cred_adarsh
WITH
    IDENTITY  = 'Managed Identity';


CREATE EXTERNAL DATA SOURCE source_silver
WITH
(
    LOCATION = 'https://awstoragedatalaket.blob.core.windows.net/silver',
    CREDENTIAL = Cred_adarsh
)



CREATE EXTERNAL DATA SOURCE source_gold
WITH
(
    LOCATION = 'https://awstoragedatalaket.blob.core.windows.net/gold',
    CREDENTIAL = Cred_adarsh
)


CREATE EXTERNAL FILE FORMAT format_parquet
WITH
(
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.GzipCodec'
)





-- CREATE EXTRNAL TABLE----

CREATE EXTERNAL TABLE gold.extsales
with 
(
    LOCATION = 'extsales',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
as 
SELECT * FROM gold.sales


SELECT * from gold.extsales
