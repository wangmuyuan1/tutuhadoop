package tutuhadoop.assignment;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WikiProjectPopularity extends Configured implements Tool {
    
    public static class WordMapper 
        extends Mapper<LongWritable, Text, Text, IntWritable> {
        
        public void map(LongWritable key, Text value, Context context)
                    throws IOException, InterruptedException{
            String[] info = value.toString().split("\\s+");
            String language = info[0];
            String project = info[1];
            int count = 0;
            try {
                count = Integer.parseInt(info[2]);
            } catch (NumberFormatException ex) {
                
            }
            context.write(new Text(language), new IntWritable(1));
        }
    }
    
    public static class SumReducer 
        extends Reducer<Text, IntWritable, Text, IntWritable> {
        
        private MultipleOutputs out;
        
        public void setup(Context context) {
            out = new MultipleOutputs(context);
        }
        
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
            
            int count = 0;
            
            for (IntWritable value : values) {
                count += value.get();
            }
            out.write(key, new IntWritable(count), "second");
        }
        
        protected void cleanup(Context context) throws IOException, InterruptedException {
            out.close();
        }
    }
    
    public static class SortMapper
        extends Mapper<LongWritable, Text, IntWritable, Text> {
        
        public void map(LongWritable key, Text value, Context context)
                    throws IOException, InterruptedException {
            String[] info = value.toString().split("\\s+");
            String project = info[0];
            int count = 0;
            try {
                count = Integer.parseInt(info[1]);
            } catch (NumberFormatException ex) {
                
            }
            context.write(new IntWritable(count), new Text(project));
        }
    }
    
    public static class SortReducer
        extends Reducer<IntWritable, Text, IntWritable, Text> {
        
        private MultipleOutputs out;
        
        public void setup(Context context) {
            out = new MultipleOutputs(context);
        }
        
        public void map(IntWritable key, Text value, Context context)
                    throws IOException, InterruptedException {
            out.write(key, value, "final");
        }
        
        protected void cleanup(Context context) throws IOException, InterruptedException {
            out.close();
        }
    }

    public static class DecreasingComparator implements RawComparator<IntWritable> {
        private IntWritable first;
        private IntWritable second;
        private DataInputBuffer buffer;
        
        public DecreasingComparator(){
            first = new IntWritable();
            second = new IntWritable();
            buffer = new DataInputBuffer();
        }

        @Override
        public int compare(IntWritable o1, IntWritable o2) {
            return -1 * (o1.get() - o2.get());
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            try {
                buffer.reset(b1, s1, l1);                  
                first.readFields(buffer);
                
                buffer.reset(b2, s2, l2);                   
                second.readFields(buffer);
                
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            return this.compare(first, second);
        }
    }
    
    public int run(String[] args) throws Exception {
        
        if (args.length != 3) {
            System.err.println("Usage: WikiProjectPopularity <input path> <temporary path> <output path>");
            System.exit(-1);
        }
        
        Configuration conf = getConf();
        
        Job job1 = WikiPreparation.getJob(args, conf);

        ControlledJob ctrljob1 = new ControlledJob(conf);   
        ctrljob1.setJob(job1);
        
        FileInputFormat.addInputPath(job1, new Path(args[0]));   
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        
        Job job2 = Job.getInstance(conf, "WikiProjectPopularity");
        
        job2.setJarByClass(WikiProjectPopularity.class);
        
        job2.setMapperClass(WordMapper.class);
        job2.setReducerClass(SumReducer.class);
        
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        
        ControlledJob ctrljob2 = new ControlledJob(conf);   
        ctrljob2.setJob(job2);
        ctrljob2.addDependingJob(ctrljob1);

        FileInputFormat.addInputPath(job2, new Path(args[1] + "/*"));
        FileOutputFormat.setOutputPath(job2,  new Path(args[2]));
        
        Job job3 = Job.getInstance(conf, "wikiProjectPopularity-sort");
        job3.setJarByClass(WikiProjectPopularity.class);
        job3.setMapperClass(SortMapper.class);
        job3.setSortComparatorClass(DecreasingComparator.class);
        
        job3.setOutputKeyClass(IntWritable.class);
        job3.setOutputValueClass(Text.class);
        
        ControlledJob ctrljob3 = new ControlledJob(conf);   
        ctrljob3.setJob(job3);
        ctrljob3.addDependingJob(ctrljob2);
        
        FileInputFormat.addInputPath(job3, new Path(args[2] + "/second*"));
        FileOutputFormat.setOutputPath(job3,  new Path("wikiProjectPopularityFinal"));
        
        JobControl jobCtrl=new JobControl("myctrl");
        
        jobCtrl.addJob(ctrljob1);
        jobCtrl.addJob(ctrljob2);
        jobCtrl.addJob(ctrljob3);
        
        Thread t = new Thread(jobCtrl);   
        t.start();
        
        while(true) {               
            if(jobCtrl.allFinished()) {
                System.out.println(jobCtrl.getSuccessfulJobList());   
                jobCtrl.stop();
                break;
            }
        }
        
        return 1;
    }
    
    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new WikiProjectPopularity(), args);
        System.exit(result);
    }
    
    
}
