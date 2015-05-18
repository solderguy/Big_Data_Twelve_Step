# CREDENTIALS
#   Module: S3_makeTable.sql
#   Author: John Soper
#   Date: Apr 2015
#   Rev: 1
#
# SUMMARY
#     This is the other third component of the Big Data Twelve Step Program project
#     It creates a MySQL table in preparation for a Sqoop import
#

CREATE DATABASE IF NOT EXISTS char_rec;
USE char_rec;
DROP TABLE IF EXISTS out_s3;
CREATE TABLE out_s3 (SN VARCHAR(10), x VARCHAR(10), y VARCHAR(10));
