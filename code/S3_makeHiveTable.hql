
-- CREDENTIALS
--   Module: S3_makeHiveTable.hql
--   Author: John Soper
--   Date: Apr 2015
--   Rev: 1
--
-- SUMMARY
--     This is the third component of the Big Data Twelve Step Program project
--     It creates the Hive tables and loads one up for easier integration with 
--     Sqoop
--

CREATE DATABASE IF NOT EXISTS points;
USE points;
DROP TABLE IF EXISTS out_s3;
DROP TABLE IF EXISTS out_s4;
set hive.cli.print.header=true;
CREATE TABLE out_s3 (SN STRING, x STRING, y STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' stored as textfile;
CREATE TABLE out_s4 (SN STRING, x STRING, y STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' stored as textfile;
LOAD DATA LOCAL INPATH '../output_files/out_s2.csv' OVERWRITE INTO TABLE points.out_s3;
