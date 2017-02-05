import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class VotingCount {

    
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable count = new IntWritable();
    private Text district = new Text();
    private final static IntWritable people = new IntWritable(0);
    //private final static LongWritable longOne = new LongWritable(1);
    private String[] strs = new String[100];
           
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
        //StringTokenizer itr = new StringTokenizer(value.toString());
        //LongWritable k = (LongWritable) key;
        //district.set(itr.nextToken());
//        if (longOne.equals(key) ){
//            System.out.println("---------------");
//        }
//        System.out.println(key);
        String content = value.toString();
        strs = content.split(",");
        System.out.println(strs[0]);
        if (strs[0].equals("Code for district")){
            return;
        }
        district.set(strs[0]);
        people.set(Integer.parseInt(strs[3]));
        
        context.write(district,people);
       
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable(0);
           
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set((double)sum);
      context.write(key, result);
    }
  }

  public static class
    
    
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "voting count");
    job.setJarByClass(VotingCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
