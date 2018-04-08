# CS455 - Hadoop research across US flights

#### By Joseph Larson

To clean: ant clean

To build: ant dist

Assumption Made: 

Question 1 and 2: minimizing delays meant finding the lowest average delay - maximizing average led to the worst time to fly.

Question 5: Old planes are considered over 20 years old and not 20 years or older. 

Question 6: Both the origin and the destination should be considered for each flight that had a weather delay.

The answer to each question is put into an individual output file. 

For question 1, the file is named bestTimeToFly 

For question 2, the file is named worstTimeToFly

For question 3, the file is named majorHubs
        
- This file will contain the top 10 results from each of the years of data which allows us to see how it has changed over the years.

For question 4, the file is named carrierDelays

- This file will contain three top 10 lists.  I left the top 10 for average, total count, and total minute delay in there since it isn't a guarantee that the carrier with the largest delays would top each of the lists.  

For question 5, the file is named olderPlanes

- This file shows the counts for old planes and new planes and also the delay rate for both of them.

For question 6, the file is named weatherDelayCities

To generate these files, run the following command: 

$HADOOP_HOME/bin/hadoop jar dist/wordcount.jar cs455.hadoop.mainjob.MainJob /data/main /data/supplementary/airports.csv /data/supplementary/carriers.csv /data/supplementary/plane-data.csv {output file}

The thought process behind question #7 was to come up with something that the startup company would be able to sell to airline companies.  I figured that each airline company is trying to minimize their delay times as much as possible to give their customers a better experience. 

To do this, I took into account the carrier delay value for each flight and paired that with the departing city.  This means it is only reporting the portion of the delay that each company has control over.  

Once the I had the average for each company in each city, I started wondering how much delay was too much.  Obviously when I fly, any delay is too much but some could be expected.  The best way I figured to come up with this was to find the average for each airport and the standard deviation. 

The values reported from this job is the average for each company and the number of standard deviations away from the average for all the flights at a specific airport

To generate the results for question 7: 

$HADOOP_HOME/bin/hadoop jar dist/wordcount.jar cs455.hadoop.customresearch.CustomJob /data/main /data/supplementary/carriers.csv /data/supplementary/airports.csv {output file}

The next steps would be to give the paying airline company a compiled list of the results for their airline at each airport compared to the mean of that airport.