
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
import java.util.Random;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.map.RegexMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;





public class WordCount_pair {

  public static class CompositeKey implements WritableComparable<CompositeKey> {
  public String w1;
  public String w2;
  public CompositeKey() { }

  public CompositeKey(String w1,  String w2) {
    super();
    this.set(w1, w2);
  }

  public void set(String w1, String w2) {
    this.w1 = (w1 == null) ? "" : w1;
    this.w2 = (w2 == null) ? "" : w2;
  }

  public void write(DataOutput out) throws IOException {
    out.writeUTF(w1);
    out.writeUTF(w2);
  }

  public void readFields(DataInput in) throws IOException {
    w1 = in.readUTF();
    w2 = in.readUTF();
  }



  public int compareTo(CompositeKey o) {
  int w1Cmp = w1.toLowerCase().compareTo(o.w1.toLowerCase());
  int w2Cmp = w2.toLowerCase().compareTo(o.w2.toLowerCase());
  if (w1Cmp != 0)
    {
    return w1Cmp;
    }
  else
    {
      return w2Cmp;
    }
  }

  public String toString() {
    return this.w1 + " " + this.w2;
  }
}


  public static class TokenizerMapper
       extends Mapper<LongWritable, Text, CompositeKey, IntWritable>{

    private final static IntWritable count = new IntWritable(1);
    private CompositeKey wordpair = new CompositeKey();
    private final static IntWritable ans = new IntWritable(0);
    private static String lastword = new String();
    private String[] strs = new String[10000];
    private int temp = -5 ;
    private boolean flag = true;
    private int x = 0;
    private String st = new String();
    private String st2 = new String();
    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
        String ptrn = "([\\?\\(\\;\\)\\_\\:\\,\\--\\-\\!]|(\')|(\")|('s)|ly|ed|ing|ness)";
        Pattern a = Pattern.compile(ptrn);
        String content = value.toString();
        content = content.trim();
        strs = content.split(" ");
        x=strs.length;


        for(int i=0;i<x-1;i++)
        {
          if(i==0 && !lastword.equals(null) && !lastword.equals("") )
          {
              st = strs[0];
              st = strs[0].trim();
              Matcher match = a.matcher(st);
              st = match.replaceAll("");
              st = st.toLowerCase();
              //System.out.println(strs[0]+lastword);
              if(st.equals(null) || lastword.equals(null) || st.equals("") || lastword.equals(""))
              {
                return;
              }
              temp = st.compareTo(lastword);
              if(temp<=0)
              {
                wordpair.set(st,lastword);
              }
              else
              {
                wordpair.set(lastword,st);
              }
              context.write(wordpair,count);
          }

          st = strs[i];
          st = strs[i].trim();
          Matcher m = a.matcher(st);
          st = m.replaceAll("");
          st = st.toLowerCase();

          st2 = strs[i+1];
          st2 = strs[i+1].trim();
          Matcher m2 = a.matcher(st2);
          st2 = m2.replaceAll("");
          st2 = st2.toLowerCase();

          if(st.equals(null) || st2.equals(null) || st.equals("") || st2.equals(""))
          {
            return;
          }

          temp = st.compareTo(st2);
          if(temp<=0)
          {
            wordpair.set(st,st2);
          }
          else
          {
            wordpair.set(st2,st);
          }
          context.write(wordpair,count);
        }
        st2 = strs[x-1];
        st2 = strs[x-1].trim();
        Matcher m3 = a.matcher(st2);
        st2 = m3.replaceAll("");
        st2 = st2.toLowerCase();
        lastword = st2;

    }
  }


  public static class sortMapper
       extends Mapper<LongWritable, Text, LongWritable, CompositeKey>{

    //private final static IntWritable one = new IntWritable(1);
    private CompositeKey rev_wordpair = new CompositeKey();
    private static LongWritable count = new LongWritable();
    private String w1 = new String();
    private String w2 = new String();
    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {

        w1=itr.nextToken();
        w2=itr.nextToken();
        if(w1.equals(null) || w2.equals(null) || w1.equals("") || w2.equals(""))
        {
          return;
        }
        rev_wordpair.set(w1,w2);
        count.set(Long.parseLong(itr.nextToken()));
      //  System.out.println(w1+ w2 + ""+count);
        context.write(count, rev_wordpair);
      }
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

  public static class DescendingIntComparator extends WritableComparator {
  public DescendingIntComparator()
  {
    super(LongWritable.class, true);
  }
  @SuppressWarnings("rawtypes")
  @Override
  public int compare(WritableComparable w1, WritableComparable w2)
  {
    LongWritable key1 = (LongWritable) w1;
    LongWritable key2 = (LongWritable) w2;
    return -1 * key1.compareTo(key2);
  }
}

  public static void main(String[] args) throws Exception {

    Path tempDir =
      new Path("grep-temp-"+
          Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

    Configuration conf = new Configuration();

    try {

    Job job = Job.getInstance(conf, "wordcount_pair");
    job.setJarByClass(WordCount_pair.class);

    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);

    job.setMapOutputKeyClass(CompositeKey.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setOutputKeyClass(CompositeKey.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, tempDir);
    job.waitForCompletion(true);


    Job sortJob = Job.getInstance(conf,"Decreasing WCP");
    sortJob.setJarByClass(WordCount_pair.class);
    FileInputFormat.setInputPaths(sortJob, tempDir);
    //sortJob.setInputFormatClass(TextInputFormat.class);

    sortJob.setMapOutputKeyClass(LongWritable.class);
    sortJob.setMapOutputValueClass(CompositeKey.class);
    sortJob.setOutputKeyClass(LongWritable.class);
    sortJob.setOutputValueClass(CompositeKey.class);
    sortJob.setMapperClass(sortMapper.class);
    sortJob.setNumReduceTasks(1);
    FileOutputFormat.setOutputPath(sortJob, new Path(args[1]));
    //System.exit(job.waitForCompletion(true) ? 0 : 1);
    sortJob.setSortComparatorClass(DescendingIntComparator.class);
    sortJob.waitForCompletion(true);
  }
  finally {
    FileSystem.get(conf).delete(tempDir, true);
  }


  }
}
