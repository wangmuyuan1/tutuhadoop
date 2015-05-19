package tutuhadoop.lab03;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class WordCountWithMultiOutput {
    
    public static class WordMapper 
        extends Mapper<LongWritable, Text, Text, IntWritable> {

        private MultipleOutputs multipleOutputs;

        protected void setup(
                Context context)
                throws IOException, InterruptedException {
            multipleOutputs = new MultipleOutputs(context);
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String s = value.toString();

            for (String word : s.split("\\W+")) {
                if (word.length() > 0) {
                    if (isStartWithLower(word)) {
                        multipleOutputs.write(NullWritable.get(), value,
                                "lower");
                    } else if (isStartWithUpper(word)) {
                        multipleOutputs.write(NullWritable.get(), value,
                                "upper");
                    } else {
                        multipleOutputs.write(NullWritable.get(), value,
                                "other");
                    }
                }
            }
        }


        protected void cleanup(Context context) throws IOException,
                InterruptedException {
            multipleOutputs.close();
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
    
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: WordCountWithMultiOutput <input path> <output path>");
            System.exit(-1);
        }
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Lab03WordCountWithMultiOutput");
        
        job.setJarByClass(WordCountWithMultiOutput.class);
        
        job.setMapperClass(WordMapper.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,  new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
