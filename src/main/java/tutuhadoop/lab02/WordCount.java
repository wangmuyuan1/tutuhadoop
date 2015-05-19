package tutuhadoop.lab02;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool {
	
	public static class WordMapper 
		extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		public void map(LongWritable key, Text value, Context context)
		            throws IOException, InterruptedException{
			String s = value.toString();
			
			for(String word : s.split("\\W+") ){
				if(word.length() > 0){
					context.write(new Text(word), new IntWritable(1));
				}
			}	
		}
	}
	
	public static class SumReducer 
		extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
			
			int wordCount = 0;
			
			for (IntWritable value : values) {
				wordCount += value.get();
			}
			context.write(key, new IntWritable(wordCount));
		}
	}
	
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: WordCount <input path> <output path>");
			System.exit(-1);
		}
		
	    Job job = Job.getInstance(getConf(), "word count");
	    
		job.setJarByClass(WordCount.class);
		
		job.setMapperClass(WordMapper.class);
		job.setReducerClass(SumReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,  new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new WordCount(), args);
		System.exit(result);
	}
}
