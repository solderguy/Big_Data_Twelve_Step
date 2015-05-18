# CREDENTIALS
#   Module: S4_copyTable.sql
#   Author: John Soper
#   Date: Apr 2015
#   Rev: 1
#
# SUMMARY
#     This is the fourth component of the Big Data Twelve Step Program project
#     It creates a copy a MySQL table and removes nulls in preparation for
#     a Sqoop Export
#

USE char_rec;
DROP TABLE IF EXISTS out_s4;
CREATE TABLE out_s4 (SN VARCHAR(10), x VARCHAR(10), y VARCHAR(10));
INSERT INTO out_s4 SELECT * FROM out_s3 WHERE x IS NOT NULL AND y is NOT NULL;
SELECT * FROM out_s4 LIMIT 10;
