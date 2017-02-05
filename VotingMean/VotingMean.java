import java.io.IOException;
import java.util.StringTokenizer;
import java.io.BufferedReader;
import java.io.*;
import java.io.InputStreamReader;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.google.common.base.Charsets;

public class VotingMean {

  private final static Text COUNT = new Text("count");
  private static double mean = 0;

  public static class TokenizerMapper
       extends Mapper<LongWritable, Text, Text, IntWritable>{

    private final static IntWritable count = new IntWritable();
    private Text district = new Text();

    private final static IntWritable people = new IntWritable(0);
    private final static LongWritable longOne = new LongWritable(1);
    private String[] strs = new String[30];

    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {

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
      result.set(sum);
      context.write(key, result);
    }
  }

  private static double readAndCalcMean(Path path, Configuration conf)
      throws IOException {
    FileSystem fs = FileSystem.get(conf);
    Path file = new Path(path, "part-r-00000");
    if (!fs.exists(file))
      throw new IOException("Output not found!");
    BufferedReader br = null;
    try {
      br = new BufferedReader(new InputStreamReader(fs.open(file), Charsets.UTF_8));

      long count = 0;
      long length = 0;

      String line;
      while ((line = br.readLine()) != null) {

        StringTokenizer st = new StringTokenizer(line);
          count = count + 1;
          String temp = st.nextToken();
          String lengthLit = st.nextToken();
          length = length + Long.parseLong(lengthLit);
      }
      double theMean = (((double) length) / ((double) count));
      return theMean;
    } finally {
      if (br != null) {
        br.close();
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "VotingMean");
    job.setJarByClass(VotingMean.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    Path outputpath = new Path(args[1]);
    boolean result = job.waitForCompletion(true);
    mean = readAndCalcMean(outputpath, conf);
    try
    {
    PrintWriter writer = new PrintWriter(outputpath+"/ans.txt", "UTF-8");
    writer.println(mean);
    writer.close();
    }
    System.exit(result ? 0 : 1);
  }
}