package cs455.hadoop.mainjob;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AirportsMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if(key.equals(new LongWritable(0))) {
            return;
        }
        String valueConvertedToString = value.toString();

        String[] individualValues = valueConvertedToString.split(",");

        if(individualValues.length > 2) {
            String mapperKey = individualValues[0];
            mapperKey = mapperKey.replace("\"", "");
            String airportName = individualValues[1].replace("\"", "");
            String airportCity = individualValues[2].replace("\"", "");

            context.write(new Text(mapperKey), new Text("F1-" + airportName + "," +airportCity));
        }
    }
}
