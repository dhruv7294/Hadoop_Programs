

import java.io.IOException;
import java.util.StringTokenizer;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TaxCount {

  public static class CompositeKey implements WritableComparable<CompositeKey> {
  public String state;
  public int level;
  public CompositeKey() { }

  public CompositeKey(String state,  int level) {
    super();
    this.set(state, level);
  }

  public void set(String state, int level) {
    this.state = (state == null) ? "" : state;
    this.level = level;
  }

  public void write(DataOutput out) throws IOException {
    out.writeUTF(state);
    out.writeInt(level);
  }

  public void readFields(DataInput in) throws IOException {
    state = in.readUTF();
    level = in.readInt();
  }



  public int compareTo(CompositeKey o) {
  int stateCmp = state.toLowerCase().compareTo(o.state.toLowerCase());
  if (stateCmp != 0)
    {
    return stateCmp;
    }
  else
    {
      return Integer.compare(level, o.level);
    }
  }

  public String toString() {
    return this.state + " " + this.level;
  }
}


  public static class TokenizerMapper
       extends Mapper<LongWritable, Text, CompositeKey, IntWritable>{

    private final static IntWritable count = new IntWritable();
    private CompositeKey district = new CompositeKey();
    private final static IntWritable tax = new IntWritable(0);

    private String[] strs = new String[10000];
    private String[] temp = new String[5];
    private float x = 0;
    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {

        String content = value.toString();
        strs = content.split(",");

        if (strs[1].equals("STATE")){
            return;
        }

        district.set(strs[1],Integer.parseInt(strs[3]));
        System.out.println(strs[4]);
        temp = strs[4].toString().split("\\.");
        tax.set(Integer.parseInt(temp[0]));
        context.write(district,tax);

    }
  }

  public static class IntSumReducer
       extends Reducer<CompositeKey,IntWritable,CompositeKey,IntWritable> {
    private IntWritable result = new IntWritable(0);

    public void reduce(CompositeKey key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }

      result.set(sum);
      context.write(key, result);
    }
  }


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "voting count");
    job.setJarByClass(TaxCount.class);

    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);

    job.setMapOutputKeyClass(CompositeKey.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setOutputKeyClass(CompositeKey.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
