# CS455 - Hadoop research across US flights

#### By Joseph Larson

Assumption Made: 

Question 1 and 2: minimizing delays meant finding the lowest average delay.

Question 5: Old planes are considered over 20 years old and not 20 years or older. 

Question 6: Both the origin and the destination should be considered for each flight that had a weather delay.

The answer to each question is put into an individual output file. 

For question 1, the file is named bestTimeToFly 

For question 2, the file is named worstTimeToFly

For question 3, the file is named majorHubs

For question 4, the file is named carrierDelays

For question 5, the file is named olderPlanes

For question 6, the file is named weatherDelayCities

To generate these files, run the following command: 

$HADOOP_HOME/bin/hadoop jar dist/wordcount.jar cs455.hadoop.mainjob.MainJob /data/main /data/supplementary/airports.csv /data/supplementary/carriers.csv /data/supplementary/plane-data.csv /home/cs455/jlarso11-hw3-output

The thought process behind question #7 was to come up withsomething that the startup company would be able to sell to airline companies.  