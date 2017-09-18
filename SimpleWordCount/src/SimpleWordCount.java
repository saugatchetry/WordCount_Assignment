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

public class SimpleWordCount {
    
    

    public static void main(String[] args) throws Exception {
        Configuration wordConf = new Configuration();
        Job wordJob = new Job(wordConf, "wordcount");
        wordJob.setJarByClass(SimpleWordCount.class);
        wordJob.setOutputKeyClass(Text.class);
        wordJob.setOutputValueClass(IntWritable.class);
        wordJob.setMapperClass(WordMapper.class);
        wordJob.setReducerClass(WordReducer.class);
        wordJob.setInputFormatClass(TextInputFormat.class);
        wordJob.setOutputFormatClass(TextOutputFormat.class);
        wordJob.setNumReduceTasks(1);

        FileInputFormat .setInputPaths(wordJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(wordJob, new Path("simpleoWordCount.txt"));

        boolean success = wordJob.waitForCompletion(true);
        System.out.println(success);
    }
}