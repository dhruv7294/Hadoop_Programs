/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 import java.util.regex.Matcher;
 import java.util.regex.Pattern;
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
//import org.apache.hadoop.mapreduce.lib.*;

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

public class WordCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private String temp = new String();
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String ptrn = "([\\?\\(\\;\\)\\_\\:\\,\\--\\-\\!]|(\')|(\")|('s)|ly|ed|ing|ness)";
       Pattern x = Pattern.compile(ptrn);
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        temp = itr.nextToken();
        temp = temp.trim();
        Matcher m = x.matcher(temp);
        temp = m.replaceAll("");
        temp = temp.toLowerCase();
        //System.out.print(temp+ " ");

        if(temp.equals(null) || temp.equals(""))
        {
          return;
        }

        word.set(new Text(temp));

        context.write(word,one);
      }
    }
  }


  public static class sortMapper
       extends Mapper<Object, Text, LongWritable, Text>{

    //private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private String temp = new String();
    private static LongWritable count = new LongWritable();
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        temp = itr.nextToken();
        temp = temp.trim();
        if(temp.equals(null) || temp.equals(""))
        {
          return;
        }
        word.set(temp);
        count.set(Long.parseLong(itr.nextToken()));
        System.out.println(count);
        context.write(count, word);
      }
    }
  }


  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

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

    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    //job.setOutputFormatClass(TextOutputFormat.class);
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, tempDir);
    job.waitForCompletion(true);


    Job sortJob = Job.getInstance(conf,"Decreasing WC");
    sortJob.setJarByClass(WordCount.class);
    FileInputFormat.setInputPaths(sortJob, tempDir);
    //sortJob.setInputFormatClass(TextInputFormat.class);
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
