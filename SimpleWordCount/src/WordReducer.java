import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class WordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable wordValue = new IntWritable(0);
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values)
                sum = sum + value.get();
            wordValue.set(sum);
            context.write(key, wordValue);
        }
    }