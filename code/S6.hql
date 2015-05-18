-- CREDENTIALS
--   Module: S6.hql
--   Author: John Soper
--   Date: Apr 2015
--   Rev: 1
--
-- SUMMARY
--     This is the sixth component of the Big Data Twelve Step Program
--     It writes oute the Serial number and Y values as an AVRO file
--

DROP TABLE IF EXISTS avro_table; 
CREATE TABLE avro_table
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
TBLPROPERTIES (
    'avro.schema.literal'='{
      "namespace": "com.jsoper.avro",
      "name": "pointdata",
      "type": "record",
      "fields": [ { "name":"sn","type":"string"}, { "name":"y","type":"string"}]
    }');
INSERT OVERWRITE TABLE avro_table SELECT sn, y FROM points.out_s4
