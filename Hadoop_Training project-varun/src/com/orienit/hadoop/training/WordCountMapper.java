package com.orienit.hadoop.training;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends
		Mapper<LongWritable, Text, Text, LongWritable> {
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// Read the Line
		String line = value.toString();
		
		// Split the Line into Words
		String[] words = line.split(" ");
		
		// Assign Count(1) to Each Word
		for (String word : words) {
			context.write(new Text(word), new LongWritable(1));
		}

	}
}

















