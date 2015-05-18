#!/bin/bash


###################################################################################################
#                                                                                                 #
#                     The Big Data Twelve Step Program Execute Script                             #
#                                                                                                 #
#                            Runs in MacOS, Centos and Ubuntu                                     #
#                                                                                                 #
#                                          Rev 2                                                  #
#                        Centos and Ubuntu support added (autodetect)                             #
#                                                                                                 #
#                                 John Soper   May 2015                                           #
#                                                                                                 #
###################################################################################################

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

check_file_size () {
    if [ "$MAC" = true ]; then
        FILESIZE=$(stat -f%z $1)
    else
        FILESIZE=$(stat -c%s $1)
    fi

    if [ $FILESIZE = $2 ]; then
        echo -e "\nPassed Step $3 with filesize $FILESIZE \n"
    else
        echo -e "\nFailed Step $3 output file size $FILESIZE, expected $2"
        echo "EXITING"
        exit
    fi
}

print_pdf () { 
    if [ $SHOW_PDF = true ]; then
        rm -f ../image_files/$1.pdf
        
        if [ "$UBUNTU" = true ]; then
            r ../code/plotPDF.R $1
        else
            Rscript ../code/plotPDF.R $1
        fi

        if [ "$MAC" = true ]; then
            open ../image_files/$1.pdf &
        else
            evince ../image_files/$1.pdf &
        fi
    fi
}

###################################################################################################
#                                                                                                 #
#                                      Setup                                                      #
#                                                                                                 #
###################################################################################################

if [ $1 = pdf ]; then
    SHOW_PDF=true
else
    SHOW_PDF=false
fi


if [ "$(uname)" = "Darwin" ]; then
  echo -e "\nRunning on Mac OS\n"
  MAC=true
else
  NAME_ID="$(cat /etc/os-release | head -1)"
  if [[ $NAME_ID = *"Ubuntu"* ]]; then
     UBUNTU=true
     echo "Running on Ubuntu OS" 
  elif [[ $NAME_ID = *"CentOS"* ]]; then
     echo "Running on Centos OS" 
     CENTOS=true
  else
     echo -e "Unknown operating system error" 
     echo -e "EXITING" 
     exit
  fi
fi



if [ "x$JAVA_HOME" == "x" ]; then
     #export JAVA_HOME="$(/usr/libexec/java_home)"
     echo -e "Environment variable JAVA_HOME needs to be set\n"
     echo -e "Exiting"
     exit
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
check_command mysqladmin
check_command mysql
check_command sbt

  if [ "$UBUNTU" = TRUE ]; then
      check_command r
  else
      check_command Rscript
  fi

echo -e "\n*******  All necessary programs found, now checking jars  ************\n"


if [ "$MAC" = true ]; then
    STREAM_JAR=/usr/local/Cellar/hadoop121/1.2.1/libexec/contrib/streaming/hadoop-streaming-*.jar
else
    STREAM_JAR=/opt/hadoop/121/hadoop-1.2.1/contrib/streaming/hadoop-streaming-*.jar
fi

if [ -f $STREAM_JAR ]; then
    echo -e "Hadoop Streaming jar file found"
else
    echo -e "Hadoop Streaming jar file not found, please check definition\n"
    exit
fi

check_jar piggybank
check_jar avro
echo -e "\n*******  All necessary jars found, now check mysql  ************\n"


if [ "$(mysqladmin ping)" = "mysqld is alive" ]; then
    echo -e "Sql Server is running"
else
    echo -e "Sql Server is not running.  Please start up\n"
    echo -e "On Mac: sudo /usr/local/mysql/support-files/mysql.server start\n"
    exit
fi

  #Instruction to create MySQL user
  #one-time prep work for mysql CLI
  #mysql -u root        // add -p if there's a password
  #CREATE USER 'analyst'@'localhost' IDENTIFIED BY 'mypass';
  #GRANT ALL PRIVILEGES ON char_rec.* TO 'analyst'@'localhost' WITH GRANT OPTION;

mysql -u "analyst" "-pmypass" -e "CREATE DATABASE IF NOT EXISTS char_rec"

if [ $(mysql -u "analyst" "-pmypass"  \
	-e "SHOW DATABASES LIKE 'char_rec'" \
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


STEP1_OUT_FILE="../output_files/out_s1/out_s1.json"
rm -f $STEP1_OUT_FILE

python ../code/S1_makePoints.py

check_file_size $STEP1_OUT_FILE 103211 1

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

check_file_size $STEP2_OUT_FILE 17696 2

#head -10 $STEP2_OUT_FILE
unset STEP2_OUTPUT
unset STEP2_OUT_FILE


###################################################################################################
#                                                                                                 #
#                            Step 3 Hive and Sqoop Export to MySQL                                #
#                                                                                                 #
###################################################################################################

STEP3_OUTPUT="/user/hive/warehouse/points.db/out_s3"
STEP3_OUT_FILE="/user/hive/warehouse/points.db/out_s3/out_s2.csv"

hive -f ../code/S3_makeHiveTable.hql

mysql -u "analyst" "-pmypass" < ../code/S3_makeTable.sql

sqoop export -fs local \
--connect jdbc:mysql://localhost/char_rec \
--username analyst --password mypass \
--fields-terminated-by ',' \
--table out_s3 \
--export-dir $STEP3_OUTPUT

check_file_size $STEP3_OUT_FILE 17696 3


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
check_file_size $STEP4_OUT_FILE 17683 4a

mv  ./out_s4 ../output_files
rm *.java	 # ORM files generated by Sqoop
rm derby.log

unset STEP4_OUT_FILE

hive -e "load data local inpath \
    '../output_files/out_s4/part-m-00000' \
    OVERWRITE INTO TABLE points.out_s4;"
hive -e "SELECT * FROM points.out_s4 LIMIT 10;"

STEP4_OUT_FILE="/user/hive/warehouse/points.db/out_s4/part-m-00000"
check_file_size $STEP4_OUT_FILE 17683 4b
unset STEP4_OUT_FILE



###################################################################################################
#                                                                                                 #
#                           Step 5 Hive and Avro Write (X column)                                 #
#                                                                                                 #
###################################################################################################

#http://rishavrohitblog.blogspot.com/2014/02/convert-csv-data-to-avro-data.html

STEP5_OUT_FILE="../output_files/S5_sn_x.avro"

hive -f ../code/S5.hql
cp /user/hive/warehouse/avro_table/000000_0 ../output_files/S5_sn_x.avro

check_file_size $STEP5_OUT_FILE 12890 5
unset STEP5_OUT_FILE



###################################################################################################
#                                                                                                 #
#                           Step 6 Hive and Avro Write (Y column)                                 #
#                                                                                                 #
###################################################################################################

STEP6_OUT_FILE="../output_files/S6_sn_y.avro"

hive -f ../code/S6.hql
cp /user/hive/warehouse/avro_table/000000_0 ../output_files/S6_sn_y.avro

check_file_size $STEP6_OUT_FILE 12607 6
unset STEP6_OUT_FILE


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

check_file_size $STEP7_OUT_FILE 17690 7

unset STEP7_OUTPUT
unset STEP7_OUT_FILE

print_pdf step7

###################################################################################################
#                                                                                                 #
#                                   Step 8 R Outlier Removal                                      #
#                                                                                                 #
###################################################################################################

STEP8_OUT_FILE="../output_files/s8out_h.csv"

if [ "$UBUNTU" = true ]; then
    r ../code/S8.R
else
    Rscript ../code/S8.R
fi

rm -rf $STEP8_OUT_FILE 
echo "x, y, sn" > ../output_files/s8out_h.csv
cat ../output_files/s8_out.csv >> $STEP8_OUT_FILE

check_file_size $STEP8_OUT_FILE 1931 8
unset STEP8_OUT_FILE

sleep 2
print_pdf step8



###################################################################################################
#                                                                                                 #
#                              Step 9 Hadoop Noisy Point Cleanup                                  #
#                                                                                                 #
###################################################################################################

cd ../code/S9_to_11_Java
rm -rf out_s9
rm -f out_s8/*.csv
cp ../../output_files/s8_out.csv ./out_s8 

PROGRAM="target/TwelveStepHadoop-0.0.1-SNAPSHOT-jar-with-dependencies.jar"

if [ ! -f $PROGRAM ]; then
    echo "Creating jar file with depencencies"
    mvn clean dependency:copy-dependencies package
fi

java -cp $PROGRAM jsoper.pair.driver.HadoopDriver pointcleaner out_s8 out_s9

rm -f ./out_s9/s9out.csv
echo "x, y" > ./out_s9/s9out.csv
cat ./out_s9/final/part-r-00000 | tr -d '\t' | grep -v '^$' > ./out_s9/s9out.csv

STEP9_OUT_FILE="../../output_files/s9out_h.csv"
echo "x, y" > $STEP9_OUT_FILE
cat ./out_s9/s9out.csv >> $STEP9_OUT_FILE
check_file_size $STEP9_OUT_FILE 1013 9
unset STEP9_OUT_FILE

cd ../../scripts
print_pdf step9


###################################################################################################
#                                                                                                 #
#                                      Step 10                                                    #
#                                                                                                 #
###################################################################################################

cd ../code/S9_to_11_Java

STEP10_OUT_FILE="../../output_files/s10out.csv"
rm -rf out_s10/*

java -cp $PROGRAM jsoper.pair.driver.HadoopDriver kmeans out_s9/s9out.csv out_s10/s10out.csv

rm -f $STEP10_OUT_FILE
cp ./out_s10/s10out.csv $STEP10_OUT_FILE

check_file_size $STEP10_OUT_FILE 2028 10
unset STEP10_OUT_FILE

cd ../../scripts
print_pdf step10
cd ../code/S9_to_11_Java



###################################################################################################
#                                                                                                 #
#                                      Step 11                                                    #
#                                                                                                 #
###################################################################################################

rm -rf out_s11
java -cp $PROGRAM jsoper.pair.driver.HadoopDriver seqfile out_s10 out_s11

check_file_size "./out_s11/part-m-00000" 3478 11
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

rm -rf ../code/S12_Spark_Char_Rec/outS11
cp ../code/S9_to_11_Java/out_s11/part-m-00000 ../code/S12_Spark_Char_Rec/outS11

# Run Spark program to decode clusters into text
cd ../code/S12_Spark_Char_Rec
sbt "run outS11 outS12.txt"

cd ../../scripts
cp ../code/S12_Spark_Char_Rec/outS12.txt ../output_files
STR=$(<../output_files/outS12.txt)
check_file_size "../output_files/outS12.txt" 8 12



# Must write R file inline because of runtime STR text parse value
STEP12_OUT_FILE=../code/S12.R
echo "d<-read.csv(\"../output_files/s10out.csv\", as.is=TRUE)" > $STEP12_OUT_FILE
echo "pdf(file=\"../image_files/step12.pdf\")" >> $STEP12_OUT_FILE
echo "plot(d\$x,d\$y, col=d\$cluster+2, xlim=c(0,119), ylim=c(0,119))" >> $STEP12_OUT_FILE
echo "title(main=\"Step 12: Decoded text: $STR\")" >> $STEP12_OUT_FILE
echo "dev.off()" >> $STEP12_OUT_FILE

Rscript ../code/S12.R

if [ "$UBUNTU" = true ]; then
    r ../code/S12.R 
else
    Rscript ../code/S12.R
fi

if [ "$MAC" = true ]; then
   open ../image_files/step12.pdf
else
   evince ../image_files/step12.pdf
fi

rm -f derby.log

echo -e "Successfully Finished Run\n"
