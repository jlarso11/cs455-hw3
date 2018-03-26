package cs455.hadoop.mindelay;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static cs455.hadoop.utils.TypeCheckUtil.isInteger;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class BestMonthToFlyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if(key.equals(new LongWritable(0))) {
            return;
        }
        String valueConvertedToString = value.toString();

        String[] individualValues = valueConvertedToString.split(",");

        int totalDelay = 0;

        if (isInteger(individualValues[14])) {
            totalDelay += Integer.parseInt(individualValues[14]);
        }
        if (isInteger(individualValues[15])) {
            totalDelay += Integer.parseInt(individualValues[15]);
        }

        String mapKey = individualValues[1] ; // + "," + individualValues[3] + "," + hour;

        context.write(new Text(mapKey), new IntWritable(totalDelay));

    }
}
