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

The thought process behind question #7 was to come up withsomething that the startup company would be able to sell to airline companies.  I figured that each airline company is trying to minimize their delay times as much as possible to give their customers a better experience. 

To do this, I took into account the carrier delay value for each flight and paired that with the city.  This means it is only reporting the portion of the delay that each company has control over.  

Once the I had the average for each company in each city, I started wondering how much delay was too much.  Obviously when I fly, any delay is too much but some could be expected.  The best way I figured to come up with this was to find the average for each airport and the standard deviation. 

The values reported from this job is the average for each company and the number of standard deviations away from the average for all the flights at a specific airport

To generate the results for question 7: 

