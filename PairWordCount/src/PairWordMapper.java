import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class PairWordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
       
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String input = value.toString().toLowerCase().trim();
        	String[] line = input.split("[\\s+ \n]+");
            String previous;
            previous = null;
            for(String s : line){
            	if(previous!=null){
            		word.set(previous+ " " +s);
                	context.write(word, one);
            	}
            	previous = s;
            }
        }
    }