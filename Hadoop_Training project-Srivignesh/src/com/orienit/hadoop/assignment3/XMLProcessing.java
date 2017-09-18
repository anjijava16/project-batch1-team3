package com.orienit.kalyan.hadoop.training.mapreduce.assignment3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class XMLProcessing {

 public static void main(String[] args) throws Exception {
  Configuration conf = new Configuration();

  conf.set("xmlinput.start", "<kalyan>");
  conf.set("xmlinput.end", "</kalyan>");
  Job job = new Job(conf);
  job.setJarByClass(XMLProcessing.class);
  job.setOutputKeyClass(Text.class);
  job.setOutputValueClass(Text.class);
  job.setMapperClass(Map.class);
  job.setNumReduceTasks(0);

  job.setInputFormatClass(XmlInputFormat.class);
  job.setOutputFormatClass(TextOutputFormat.class);

  FileInputFormat.addInputPath(job, new Path(args[0]));
  FileOutputFormat.setOutputPath(job, new Path(args[1]));

  job.waitForCompletion(true);
 }
}
