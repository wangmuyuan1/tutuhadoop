package tutuhadoop.lab01;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountAverage {

    public static class WordMapper extends
            Mapper<LongWritable, Text, Text, DoubleWritable> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String s = value.toString();

            for (String word : s.split("\\W+")) {
                if (word.length() > 0) {
                    context.write(new Text(word.toUpperCase().substring(0, 1)),
                            new DoubleWritable(word.length()));
                }
            }
        }
    }

    public static class SumReducer extends
            Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        public void reduce(Text key, Iterable<DoubleWritable> values,
                Context context) throws IOException, InterruptedException {

            int wordCount = 0;
            double totalLength = 0;

            for (DoubleWritable value : values) {
                totalLength += value.get();
                wordCount++;
            }

            double result = totalLength / wordCount;
            System.out.println("Key: " + key + ", Result: " + result);
            context.write(key, new DoubleWritable(result));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err
                    .println("Usage: WordCountAverage <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");

        job.setJarByClass(WordCountAverage.class);

        job.setMapperClass(WordMapper.class);
        job.setReducerClass(SumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
