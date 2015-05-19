package tutuhadoop.lab02;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountWithCounter extends Configured implements Tool {

	public static class WordMapper 
		extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		static enum MyCounters{UPPER, LOWER}
		
		public void map(LongWritable key, Text value, Context context)
		            throws IOException, InterruptedException{
			String s = value.toString();
			
			for(String word : s.split("\\W+") ){
				if(word.length() > 0){
					if (isStartWithUpper(word)) 
					{
						context.getCounter(MyCounters.UPPER).increment(1);
					}
					else if (isStartWithLower(word))
					{
						context.getCounter(MyCounters.LOWER).increment(1);
					}
				}
			}	
		}
		
		private boolean isStartWithUpper(String s) {
			char first = s.charAt(0);
			return (first >= 65 && first <= 90);
		}
		
		private boolean isStartWithLower(String s) {
			char first = s.charAt(0);
			return (first >= 97 && first <= 122);
		}
	}
	
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: WordCountWithCounter <input path> <output path>");
			System.exit(-1);
		}
		
	    Job job = Job.getInstance(getConf(), "word count");
	    
		job.setJarByClass(WordCount.class);		
		job.setMapperClass(WordMapper.class);
	
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		int result = job.waitForCompletion(true) ? 0 : 1;
		

		System.out.println("Job is complete - printing counters now:");
		Counters counters = job.getCounters();
		Counter upper = counters.findCounter(WordMapper.MyCounters.UPPER);
		System.out.println("Number of words start with uppercase: "+ upper.getValue());
		
		Counter lower = counters.findCounter(WordMapper.MyCounters.LOWER);
		System.out.println("Number of words start with lowercase: "+ lower.getValue());
		
		return result;
	}
	
	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new WordCountWithCounter(), args);
		System.exit(result);
	}
}
