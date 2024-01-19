package kubecluster.org;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCountDemo {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }
        Job application = Job.getInstance(conf, "word count");
        application.setJarByClass(WordCountDemo.class);
        application.setMapperClass(WordCountMapper.class);
        application.setCombinerClass(WordCountReducer.class);
        application.setReducerClass(WordCountReducer.class);
        application.setOutputKeyClass(Text.class);
        application.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(application, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(application, new Path(otherArgs[otherArgs.length - 1]));
        System.exit(application.waitForCompletion(true) ? 0 : 1);
    }
}
