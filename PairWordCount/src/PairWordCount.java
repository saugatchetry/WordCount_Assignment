import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PairWordCount {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job pairjWordJob = new Job(conf, "wordcount");
        pairjWordJob.setJarByClass(PairWordCount.class);
        pairjWordJob.setOutputKeyClass(Text.class);
        pairjWordJob.setOutputValueClass(IntWritable.class);
        pairjWordJob.setMapperClass(PairWordMapper.class);
        pairjWordJob.setReducerClass(PairWordReducer.class);
        pairjWordJob.setInputFormatClass(TextInputFormat.class);
        pairjWordJob.setOutputFormatClass(TextOutputFormat.class);
        pairjWordJob.setNumReduceTasks(1);

        FileInputFormat .setInputPaths(pairjWordJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(pairjWordJob, new Path("PairWordCount.txt"));

        boolean success = pairjWordJob.waitForCompletion(true);
        System.out.println(success);
    }
}