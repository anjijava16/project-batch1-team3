package com.orienit.kalyan.hadoop.training.grep.multiplefiles;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GrepMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// Read the line
		String line = value.toString();

		// Get the pattern
		Configuration conf = context.getConfiguration();
		String pattern = conf.get("grep.pattern");

		// If line contains pattern
		// then print it, otherwise skip it
		if (line.contains(pattern)) {
			context.write(value, NullWritable.get());
		}
	}
}
