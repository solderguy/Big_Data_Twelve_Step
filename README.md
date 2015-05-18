To Be Done:  Centos support, output file size checking, expand logging, unit tests

Can chemically dependent points become productive members of a text society?

This project was intended to deepen my skills in the Hadoop Ecosystem.  
Output PDFs are available in image_files folder  
The bash script run_me.sh fires up twelve tools:      

1.  <b>Python</b> program encodes bitmap characters into JSON format with numerous flaws  
2.  Hadoop **Streaming** discards invalid JSON records  
3.  Points are imported into a <b>Hive</b> table, then transferred to MySQL with Sqoop  
4.  <b>MySQL</b> script copies the points into another table while removing nulls   
5.  <b>Sqoop</b> transfer back to Hive, X and Y values of points are then split up  
6.  Hive writes out two files in <b>AVRO</b> format  
7.  <b>Pig</b> reads both files and joins on the serial number  
8.  <b>R</b> removes bitmap outliers  
9.  Two <b>Hadoop</b> MR runs remove all the noise points leaving only character information  
10.  <b>Mahout</b> performs K-means to create a cluster for each character  
11.  Points with cluster IDs written out in <b>Sequence File</b> format  
12.  <b>Spark</b> (using <b>Scala</b> code) opens the Sequence File and decodes each cluster into text  

Verified with MacOS 10.10.1, Java 1.7, Hadoop 1.2.1, Hive 0.13.1, Pig 0.12, Mahout 0.5, Spark 1.2.1, Scala 2.11.6  
Centos support will be soon added after bash scripts are reconciled
