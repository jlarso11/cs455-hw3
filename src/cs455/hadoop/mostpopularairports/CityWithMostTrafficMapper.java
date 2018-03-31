package cs455.hadoop.mostpopularairports;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class CityWithMostTrafficMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if(key.equals(new LongWritable(0))) {
            return;
        }
        String valueConvertedToString = value.toString();

        String[] individualValues = valueConvertedToString.split(",");

        if(individualValues.length > 18) {
            String originKey = individualValues[0] + "," + individualValues[16];
            context.write(new Text(originKey), new IntWritable(1));

            String destKey = individualValues[0] + "," + individualValues[17];
            context.write(new Text(destKey), new IntWritable(1));
        }
    }
}
