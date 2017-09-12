package com.orienit.kalyan.hadoop.training.grep.multiplefiles;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GrepJob implements Tool {
	// Declare Configuration Object
	private Configuration conf;

	@Override
	public Configuration getConf() {
		return conf; // Getting the configuration
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf; // Setting the configuration
	}

	@Override
	public int run(String[] args) throws Exception {

		// initializing the job object with configuration
		Job job = new Job(getConf());

		// setting the job name
		job.setJobName("Orien IT Grep Job");

		// Set the Jar by finding where a given class came from
		job.setJarByClass(this.getClass());

		// setting custom mapper class
		job.setMapperClass(GrepMapper.class);

		// setting no of reducers
		job.setNumReduceTasks(0);

		// setting mapper output key class: K2
		job.setMapOutputKeyClass(Text.class);

		// setting mapper output value class: V2
		job.setMapOutputValueClass(NullWritable.class);

		// setting final output key class: K2
		job.setOutputKeyClass(Text.class);

		// setting final output value class: V2
		job.setOutputValueClass(NullWritable.class);

		// setting the input format class ,i.e for K1, V1
		job.setInputFormatClass(TextInputFormat.class);

		// setting the output format class
		job.setOutputFormatClass(TextOutputFormat.class);

		// define the input & output paths
		String inputs = StringUtils.join(args, ",", 0, args.length - 1);
		Path output = new Path(args[args.length - 1]);

		// setting the input path
		FileInputFormat.addInputPaths(job, inputs);

		// setting the output path
		FileOutputFormat.setOutputPath(job, output);

		// delete the output path if exists
		output.getFileSystem(conf).delete(output, true);

		// to execute the job and return the status
		return job.waitForCompletion(true) ? 0 : -1;

	}

	public static void main(String[] args) throws Exception {
		// if `status == 0` then `Job Success`
		// if `status == -1` then `Job Failure`
		Configuration conf = new Configuration();
		conf.set("grep.pattern", args[2]);

		int status = ToolRunner.run(conf, new GrepJob(), args);

		System.out.println("Job Status: " + status);
	}

}
