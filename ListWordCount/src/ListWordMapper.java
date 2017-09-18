import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class ListWordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);

		private Text word = new Text();
		private Set pattern = new HashSet<>();

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			
			URI[] patternFiles = context.getCacheFiles();
	
			if (patternFiles.length != 1) {
				System.out.println("Error");
			}
			if (patternFiles != null && patternFiles.length > 0) {
				URI patternFile = patternFiles[0];
				BufferedReader bufferedReader = new BufferedReader(
						new FileReader(patternFile.toString()));
				String patternWord = null;
				StringBuilder sb = new StringBuilder();
				while ((patternWord = bufferedReader.readLine()) != null) {
				
					sb.append(patternWord);
				}
				
				String[] inputWords = sb.toString().split("\\s");
				for(String s : inputWords){
					pattern.add(s);
					System.out.println(s);
				}
				bufferedReader.close();

			}

		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				if(pattern.contains(word.toString().toLowerCase())){		
				context.write(word, one);
				System.out.println(word);
				}
			}
		}
	}