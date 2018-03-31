package cs455.hadoop.olderplanes;

import cs455.hadoop.utils.TypeCheckUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class OldPlaneDelaysMapper1 extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if(key.equals(new LongWritable(0))) {
            return;
        }
        String valueConvertedToString = value.toString();

        String[] individualValues = valueConvertedToString.split(",");

        if(individualValues.length > 15 && TypeCheckUtil.isInteger(individualValues[0]) && TypeCheckUtil.isInteger(individualValues[14]) && TypeCheckUtil.isInteger(individualValues[15])) {
            Integer totalDelays = Integer.parseInt(individualValues[14]) + Integer.parseInt(individualValues[15]);
            String originKey = individualValues[10];
            context.write(new Text(originKey), new Text("F1-" + individualValues[0] + "," + totalDelays.toString()));
        }
    }
}
