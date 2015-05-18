Can chemically dependent points become productive members of a text society through a twelve-step program?

Output results are already available as PDFs in the image_files folder.  

The bash script run_me.sh fires up twelve tools for the twelve steps:      

1.  **Python** program encodes bitmap characters into JSON format with numerous flaws  
2.  **Hadoop Streaming** discards invalid JSON records  
3.  **Hive** table used to store points before transfer to MySQL with Sqoop  
4.  **MySQL** script copies the points into another table while removing nulls   
5.  **Sqoop** transfer back to Hive, X and Y values of points are then split up  
6.  **AVRO** files generated by Hive  
7.  **Pig** reads both files and joins on the serial number  
8.  **R** removes bitmap outliers  
9.  **Hadoop** Two MR runs remove all the noise points leaving only character information  
10. **Mahout** performs K-means to create a cluster for each character  
11. **Sequence File** used to write out points with cluster IDs  
12. **Spark with Scala** opens the Sequence File and decodes each cluster into text  

Verified with MacOS 10.10.1, Java 1.7, Hadoop 1.2.1, Hive 0.13.1, Pig 0.12, Mahout 0.5, Spark 1.2.1, Scala 2.11.6  
Centos and Ubuntu verified on VMs
