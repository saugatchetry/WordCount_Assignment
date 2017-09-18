import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ListWordCount {
	

	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job listWordJob = Job.getInstance(conf);
		listWordJob.addCacheFile(new URI(args[1]));
		listWordJob.setJarByClass(ListWordCount.class);
		listWordJob.setOutputKeyClass(Text.class);
		listWordJob.setOutputValueClass(IntWritable.class);
		listWordJob.setMapperClass(ListWordMapper.class);
		listWordJob.setReducerClass(ListWordReducer.class);
		listWordJob.setInputFormatClass(TextInputFormat.class);
		listWordJob.setOutputFormatClass(TextOutputFormat.class);
		listWordJob.setNumReduceTasks(1);

		FileInputFormat.setInputPaths(listWordJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(listWordJob, new Path("ListWordCount.txt"));

		boolean success = listWordJob.waitForCompletion(true);
		System.out.println(success);
	}
}