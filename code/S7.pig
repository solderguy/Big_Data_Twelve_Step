REGISTER piggybank.jar
REGISTER avro.jar
sn_x = LOAD '../output_files/S6_sn_x.avro' USING AvroStorage AS (sn:chararray, x:chararray);
sn_y = LOAD '../output_files/S6_sn_y.avro' USING AvroStorage AS (sn:chararray, y:chararray);
raw_joined = JOIN sn_x BY sn, sn_y BY sn;
joined = foreach raw_joined generate sn_x::sn, x, y;
STORE joined INTO '../output_files/out_s7';
