#!/bin/bash


###################################################################################################
#                                                                                                 #
#                     The Big Data Twelve Step Program Execute Script                             #
#                                                                                                 #
#                           Runs in MacOS (Centos coming soon)                                    #
#                                                                                                 #
#                                          Rev 1                                                  #
#                                                                                                 #
#                                 John Soper   Apr 2015                                           #
#                                                                                                 #
###################################################################################################

#  TBD:  check output file sizes at each step, implement into function
#  PIG remove duplicate points
#  More noise points 
#  Reconcile with Centos version


###################################################################################################
#                                                                                                 #
#                                      Functions                                                  #
#                                                                                                 #
###################################################################################################

check_command () {
  if type $1 &> /dev/null; then
      echo -e "$1 found"
  else
      echo -e "$1 not found\nEXITING"
      exit
   fi
}

check_jar () {
    if [ -f ./$1.jar ]; then
        echo -e "$1 jar file found"
    else
        echo -e "$1 jar file not found\nEXITING"
        exit
    fi
}

###################################################################################################
#                                                                                                 #
#                                      Setup                                                      #
#                                                                                                 #
###################################################################################################

if [ "$(uname)" = "Darwin" ]; then
  echo -e "\nRunning on Mac OS\n"
  MAC=true
else
  echo -e "\nRunning on Linux OS or something else besides a Mac\n"
  MAC=false
fi

if [ "x$JAVA_HOME" == "x" ]; then
     export JAVA_HOME="$(/usr/libexec/java_home)"
     echo -e "JAVA_HOME:  $JAVA_HOME"
else
     echo -e "JAVA_HOME already set to $JAVA_HOME"
fi

check_command python
check_command java
check_command hadoop
check_command mvn
check_command pig
check_command hive
check_command sqoop
check_command mysql
check_command Rscript
check_command sbt
echo -e "\n*******  All necessary programs found, now checking jars  ************\n"


STREAM_JAR=/usr/local/Cellar/hadoop121/1.2.1/libexec/contrib/streaming/hadoop-streaming-*.jar
if [ -f $STEAM_JAR ]; then
    echo -e "Hadoop Streaming jar file found"
else
    echo -e "Hadoop Streaming jar file not found, please check definition\n"
    exit
fi

check_jar piggybank
check_jar avro

echo -e "\n*******  All necessary jars found, now check mysql  ************\n"

#  HELP IF BELOW FAILS
#  sudo /usr/local/mysql/support-files/mysql.server start  # start->stop to stop

if [ "$(mysqladmin ping)" = "mysqld is alive" ]; then
    echo -e "Sql Server is running"
else
    echo -e "Sql Server is not running.  Please start up\n"
    echo -e "On Mac: sudo /usr/local/mysql/support-files/mysql.server start\n"
    exit
fi

#  HELP IF BELOW FAILS
#  one-time prep work for mysql CLI
#  mysql -u root        // add -p if there's a password
#  CREATE USER 'analyst'@'localhost' IDENTIFIED BY 'mypass';
#  GRANT ALL PRIVILEGES ON char_rec.* TO 'analyst'@'localhost' WITH GRANT OPTION;

if [ $(mysql -u "analyst" "-pmypass"  \
	-e "SHOW DATABASES LIKE 'test'" \
    		| wc -l ) -eq 2 ]; then
    echo -e "Mysql responding for username analyst\n"
else
    echo -e "Mysql not working for username analyst\n"
    exit
fi

echo -e "*******  Passed all checks  ************\n"



###################################################################################################
#                                                                                                 #
#                              Step 1 JSON file generation with Python                            #
#                                                                                                 #
###################################################################################################

#if false; then

echo -e "*******  Running Step 1  ************\n"

STEP1_OUT_FILE="../output_files/out_s1/out_s1.json"
rm -f $STEP1_OUT_FILE

python ../code/S1_makePoints.py

if [ "$MAC" = true ]; then
    FILESIZE=$(stat -f%z $STEP1_OUT_FILE)
else
    FILESIZE=$(stat -c%s $STEP1_OUT_FILE)
fi

if [ $FILESIZE != 11060 ]; then
    echo -e "Passed Step 1 with json filesize $FILESIZE \n"
else
    echo -e "Failed Step 1 with json filesize $FILESIZE \n"
    echo "EXITING"
    exit
fi

unset STEP1_OUT_FILE



###################################################################################################
#                                                                                                 #
#                     Step 2 Clean up invalid points with Hadoop Streaming                        #
#                                                                                                 #
###################################################################################################

STEP2_OUTPUT="../output_files/out_s2"
STEP2_OUT_FILE="../output_files/out_s2.csv"

rm -f STEP2_OUT_FILE
rm -rf $STEP2_OUTPUT

hadoop jar $STREAM_JAR \
-input ../output_files/out_s1 \
-output $STEP2_OUTPUT \
-mapper ../code/S2_mapper.sh \
-reducer org.apache.hadoop.mapred.lib.IdentityReducer

cat $STEP2_OUTPUT/p* | tr -d '\t' > $STEP2_OUT_FILE

head -10 $STEP2_OUT_FILE

ls -l $STEP2_OUT_FILE


###################################################################################################
#                                                                                                 #
#                            Step 3 Hive and Sqoop Export to MySQL                                #
#                                                                                                 #
###################################################################################################


#  one-time prep work for mysql CLI
#  mysql -u root        // add -p if there's a password
#  CREATE DATABASE char_rec;
#  CREATE USER 'analyst'@'localhost' IDENTIFIED BY 'mypass';
#  GRANT ALL PRIVILEGES ON char_rec.* TO 'analyst'@'localhost' WITH GRANT OPTION;

# cut and paste below to start mysql server
# sudo /usr/local/mysql/support-files/mysql.server start    // start->stop to stop

hive -f ../code/S3_makeHiveTable.hql

mysql -u "analyst" "-pmypass" < ../code/S3_makeTable.sql

sqoop export -fs local \
--connect jdbc:mysql://localhost/char_rec \
--username analyst --password mypass \
--fields-terminated-by ',' \
--table out_s3 \
--export-dir /user/hive/warehouse/points.db/out_s3


###################################################################################################
#                                                                                                 #
#                            Step 4 MySQL and Sqoop Import from MySQL                             #
#                                                                                                 #
###################################################################################################

mysql -u "analyst" "-pmypass" < ../code/S4_copyTable.sql

rm -rf /user/hive/warehouse/points.db/out_s4
rm -rf ./out_s4
rm -rf ../output_files/out_s4

sqoop import  \
--connect jdbc:mysql://localhost/char_rec \
--table out_s4 \
--username analyst --password mypass \
--fields-terminated-by ',' \
-m 1

STEP4_OUT_FILE="./out_s4/part-m-00000"

if [ "$MAC" = true ]; then
    FILESIZE=$(stat -f%z $STEP4_OUT_FILE)
else
    FILESIZE=$(stat -c%s $STEP4_OUT_FILE)
fi

if [ $FILESIZE != 11060 ]; then
    echo -e "Sqoop import successful with file size $FILESIZE \n"
else
    echo -e "Failed Sqoop import, file size $FILESIZE \n"
    echo "EXITING"
    exit
fi

mv  ./out_s4 ../output_files
rm *.java	 # ORM files generated by Sqoop
rm derby.log

unset STEP4_OUT_FILE

hive -e "load data local inpath \
    '../output_files/out_s4/part-m-00000' \
    OVERWRITE INTO TABLE points.out_s4;"
hive -e "SELECT * FROM points.out_s4 LIMIT 10;"

echo -e "********************** done with step 4"


###################################################################################################
#                                                                                                 #
#                           Step 5 Hive and Avro Write (X column)                                 #
#                                                                                                 #
###################################################################################################

#http://rishavrohitblog.blogspot.com/2014/02/convert-csv-data-to-avro-data.html

hive -f ../code/S6a.hql
cp /user/hive/warehouse/avro_table/000000_0 ../output_files/S6_sn_x.avro


###################################################################################################
#                                                                                                 #
#                           Step 6 Hive and Avro Write (Y column)                                 #
#                                                                                                 #
###################################################################################################

hive -f ../code/S6b.hql
cp /user/hive/warehouse/avro_table/000000_0 ../output_files/S6_sn_y.avro


###################################################################################################
#                                                                                                 #
#                              Step 7 Pig Join and Avro Read                                      #
#                                                                                                 #
###################################################################################################

STEP7_OUTPUT="../output_files/out_s7"
STEP7_OUT_FILE="../output_files/out_s7.tab"

rm -f $STEP7_OUT_FILE
rm -rf $STEP7_OUTPUT

pig -x local ../code/S7.pig

head -10 $STEP7_OUTPUT/part-r-00000

echo "sn	x	y" > $STEP7_OUT_FILE
cat $STEP7_OUTPUT/part* >> $STEP7_OUT_FILE

unset STEP7_OUTPUT
unset STEP7_OUT_FILE

rm -f ../image_files/s7.pdf
Rscript ../code/plotPDF.R step7
open ../image_files/s7.pdf




###################################################################################################
#                                                                                                 #
#                                   Step 8 R Outlier Removal                                      #
#                                                                                                 #
###################################################################################################

pwd
Rscript ../code/S8.R

# if on linux, try below
# alias open='gnome-open'

sleep 2
rm -f ../output_files/s8out_h.csv
echo "x, y, sn" > ../output_files/s8out_h.csv
cat ../output_files/s8_out.csv >> ../output_files/s8out_h.csv

rm -f ../image_files/s8.pdf
Rscript ../code/plotPDF.R step8
open ../image_files/s8.pdf



###################################################################################################
#                                                                                                 #
#                              Step 9 Hadoop Noisy Point Cleanup                                  #
#                                                                                                 #
###################################################################################################

cd ../code/S9_to_11_Java
rm -rf out_s9
rm -f out_s8/*.csv
cp ../../output_files/s8_out.csv ./out_s8 

PROGRAM="target/SeqFilePlay-0.0.1-SNAPSHOT-jar-with-dependencies.jar"

if [ ! -f $PROGRAM ]; then
    echo "Creating jar file with depencencies"
    mvn clean dependency:copy-dependencies package
fi

java -cp $PROGRAM jsoper.pair.s09hadoop.S9Driver pointcleaner out_s8 out_s9

rm -f ./out_s9/s9out.csv
echo "x, y" > ./out_s9/s9out.csv
cat ./out_s9/final/part-r-00000 | tr -d '\t' | grep -v '^$' > ./out_s9/s9out.csv

echo "x, y" > ../../output_files/s9out_h.csv
cat ./out_s9/s9out.csv >> ../../output_files/s9out_h.csv

rm -f ../../image_files/s9.pdf
Rscript ../plotPDF.R step9
open ../../image_files/s9.pdf



###################################################################################################
#                                                                                                 #
#                                      Step 10                                                    #
#                                                                                                 #
###################################################################################################

rm -rf out_s10/*

java -cp $PROGRAM jsoper.pair.s10cluster.KMeansClustering

pwd
rm ../../output_files/s10out.csv
cp ./out_s10/s10out.csv ../../output_files

rm -f ../../image_files/s10.pdf
Rscript ../plotPDF.R step10
open ../../image_files/s10.pdf



###################################################################################################
#                                                                                                 #
#                                      Step 11                                                    #
#                                                                                                 #
###################################################################################################

rm -rf out_s11
java -cp $PROGRAM jsoper.pair.s11seqfile.Text2SeqFile
cd ../../scripts

###################################################################################################
#                                                                                                 #
#                                      Step 12                                                    #
#                                                                                                 #
###################################################################################################

# Delete old files
rm -f ../image_files/s12.pdf
rm -f ../output_files/outS12.txt
rm -r ../code/S12_Spark_Char_Rec/outS12.txt
rm -f ../code/s12.R

rm -f ../code/S12_Spark_Char_Rec/outS11
cp ../code/S9_to_11_Java/out_s11/part-m-00000 ../code/S12_Spark_Char_Rec/outS11

# Run Spark program to decode clusters into text
cd ../code/S12_Spark_Char_Rec
sbt run

cd ../../scripts
cp ../code/S12_Spark_Char_Rec/outS12.txt ../output_files
STR=$(<../output_files/outS12.txt)

# Must write R file inline because of runtime STR text parse value
STEP12_OUT_FILE=../code/S12.R
echo "setwd(\"/Users/john/twelve/scripts\")" > $STEP12_OUT_FILE
echo "d<-read.csv(\"../output_files/s10out.csv\", as.is=TRUE)" > $STEP12_OUT_FILE
echo "pdf(file=\"../image_files/s12.pdf\")" >> $STEP12_OUT_FILE
echo "plot(d\$x,d\$y, col=d\$cluster+2, xlim=c(0,119), ylim=c(0,119))" >> $STEP12_OUT_FILE
echo "title(main=\"Step 12: Decoded text: $STR\")" >> $STEP12_OUT_FILE
echo "dev.off()" >> $STEP12_OUT_FILE

Rscript ../code/s12.R
open ../image_files/s12.pdf

echo "Finished Run"
