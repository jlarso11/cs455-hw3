package cs455.hadoop.customresearch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CustomJob {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            // Give the MapRed job a name. You'll see this name in the Yarn webapp.
            Job job = Job.getInstance(conf, "word count");
            // Current class.
            job.setJarByClass(CustomJob.class);

            MultipleInputs.addInputPath(job,new Path(args[0]), TextInputFormat.class, CustomMapper1.class);
            MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, CustomMapper2.class);
            MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, CustomMapper3.class);

            // Combiner. We use the reducer as the combiner in this case.
            job.setCombinerClass(CustomCombiner.class);

            // Reducer
            job.setReducerClass(CustomReducer.class);

            // Outputs from the Mapper.
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            // Outputs from Reducer. It is sufficient to set only the following two properties
            // if the Mapper and Reducer has same key and value types. It is set separately for
            // elaboration.
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            // path to output in HDFS
            FileOutputFormat.setOutputPath(job, new Path(args[3]));

            // Block until the job is completed.
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            System.err.println(e.getMessage());
        }

    }
}
