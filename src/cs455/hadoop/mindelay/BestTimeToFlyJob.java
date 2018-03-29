package cs455.hadoop.mindelay;

import cs455.hadoop.utils.AverageCombiner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * This is the main class. Hadoop will invoke the main method of this class.
 */
public class BestTimeToFlyJob {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            // Give the MapRed job a name. You'll see this name in the Yarn webapp.
            Job job = Job.getInstance(conf, "word count");
            // Current class.
            job.setJarByClass(BestTimeToFlyJob.class);

            // Mapper
            job.setMapperClass(BestTimeToFlyMapper.class);

            // Combiner. We use the reducer as the combiner in this case.
            job.setCombinerClass(AverageCombiner.class);

            // Reducer
            job.setReducerClass(BestTimeToFlyReducer.class);

            // Outputs from the Mapper.
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            // Outputs from Reducer. It is sufficient to set only the following two properties
            // if the Mapper and Reducer has same key and value types. It is set separately for
            // elaboration.
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            // path to input in HDFS
            FileInputFormat.addInputPath(job, new Path(args[0]));
            // path to output in HDFS
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            MultipleOutputs.addNamedOutput(job, "bestDay", TextOutputFormat.class, Text.class, IntWritable.class);
            MultipleOutputs.addNamedOutput(job, "bestHour", TextOutputFormat.class, Text.class, IntWritable.class);
            MultipleOutputs.addNamedOutput(job, "bestMonth", TextOutputFormat.class, Text.class, IntWritable.class);


            // Block until the job is completed.
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (IOException e) {
            System.err.println(e.getMessage());
        } catch (InterruptedException e) {
            System.err.println(e.getMessage());
        } catch (ClassNotFoundException e) {
            System.err.println(e.getMessage());
        }

    }
}
