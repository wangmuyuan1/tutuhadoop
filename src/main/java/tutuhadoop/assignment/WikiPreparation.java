package tutuhadoop.assignment;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This class shows the popularity of wikipedia projects.
 * 
 * @author Wen Wang
 */
public class WikiPreparation extends Configured implements Tool {

    public static class FirstMapper 
        extends Mapper<LongWritable, Text, MyCompositeKey, MyCompositeValue> {
        
        public void map(LongWritable key, Text value, Context context)
                    throws IOException, InterruptedException{
            String[] info = value.toString().split("\\s+");
            if (info.length < 4)
            {
                return;
            }
            String languageInfo = info[0];
            String title = info[1];
            
            if (languageInfo.contains("\\."))
            {
                // Only interested in wikipedia.
                return;
            }
            String language = getLanguage(languageInfo);
            int visit = 0;
            try {
                visit= Integer.parseInt(info[2]);
            } catch (NumberFormatException ex) {
                // Do nothing.
            }

            context.write(new MyCompositeKey(new Text(language), new Text(title)), new MyCompositeValue(new IntWritable(1), new IntWritable(visit)));
        }
        
        private String getLanguage(String languageInfo) {
            String[] info = languageInfo.split("\\.");
            return info[0].toLowerCase();
        }
    }
    
    public static class FirstReducer 
        extends Reducer<MyCompositeKey, MyCompositeValue, MyCompositeKey, MyCompositeValue> {
        
        public void reduce(MyCompositeKey key, Iterable<MyCompositeValue> values, Context context)
            throws IOException, InterruptedException {
            
            int numberOfPageTitle = 0;
            int pageVisitCount = 0;
            
            for (MyCompositeValue value : values) {
                numberOfPageTitle += value.getNumberOfPageTile().get();
                pageVisitCount += value.getPageVisitCount().get();
            }
            context.write(key, new MyCompositeValue(new IntWritable(numberOfPageTitle), new IntWritable(pageVisitCount)));
        }
    }
    
    public static class MyCompositeKey implements WritableComparable<MyCompositeKey> {
        
        private Text language = new Text("");
        private Text title = new Text("");
        
        public MyCompositeKey()
        {   
        }
        
        public MyCompositeKey(Text language, Text page)
        {
            this.language = language;
            this.title = page;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            language.write(out);
            title.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            language.readFields(in);
            title.readFields(in);
        }

        @Override
        public int compareTo(MyCompositeKey key) {
            return language.compareTo(key.language) + 31 * title.compareTo(key.title);
        }

        public Text getLanguage() {
            return language;
        }

        public Text getTitle() {
            return title;
        }
        public String toString() {
            return this.language + " " + this.title;
        }
    }
    
    public static class MyCompositeValue implements WritableComparable<MyCompositeValue> {
        private IntWritable numberOfPageTitle = new IntWritable();
        private IntWritable pageVisitCount = new IntWritable();
        
        public MyCompositeValue() {
        }
        
        public MyCompositeValue(IntWritable numberOfPageTitle, IntWritable pageVisitCount) {
            this.numberOfPageTitle = numberOfPageTitle;
            this.pageVisitCount = pageVisitCount;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            numberOfPageTitle.write(out);
            pageVisitCount.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            numberOfPageTitle.readFields(in);
            pageVisitCount.readFields(in);
        }

        @Override
        public int compareTo(MyCompositeValue value) {
            return numberOfPageTitle.compareTo(value.numberOfPageTitle) + 31 * pageVisitCount.compareTo(value.pageVisitCount);
        }
        
        public String toString() {
            return this.numberOfPageTitle + " " + this.pageVisitCount;
        }

        public IntWritable getNumberOfPageTile() {
            return numberOfPageTitle;
        }

        public IntWritable getPageVisitCount() {
            return pageVisitCount;
        }
    }
    
    public static Job getJob(String[] args, Configuration conf) throws Exception {
        
        Job job = Job.getInstance(conf, "WikiPrepartion");
        
        job.setJarByClass(WikiPreparation.class);
        
        job.setMapperClass(FirstMapper.class);
        job.setCombinerClass(FirstReducer.class);
        job.setReducerClass(FirstReducer.class);
        
        job.setOutputKeyClass(MyCompositeKey.class);
        job.setOutputValueClass(MyCompositeValue.class);
        
        return job;
    }
    
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: WikiPrepartion <input path> <output path>");
            System.exit(-1);
        }
        
        Job job = getJob(args, getConf());
        
        FileInputFormat.addInputPath(job, new Path(args[0]));   
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        return job.waitForCompletion(true) ? 0 : 1;
    }
    
    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new WikiPreparation(), args);
        System.exit(result);
    }
}
