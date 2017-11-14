package com.hadoop.poc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
public class Driver {


	public static void main(String... args) throws Exception
	{
		//System.out.println("abc");
		Configuration conf=new Configuration();
		Job job=new Job(conf);
		job.setJarByClass(Driver.class);
		job.setMapperClass(RecordMapper.class);
	//	System.out.println("abc2");
		job.setReducerClass(RecordReducer.class);
		job.setInputFormatClass(ExcelInputFormat.class);
		job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
	//	job.setPartitionerClass(RecordPart.class);
		//System.out.println("abc1");		
		MultipleOutputs.addNamedOutput(job, "HighBuzzProducts", TextOutputFormat.class,Text.class,Text.class);
		MultipleOutputs.addNamedOutput(job, "NormalProducts", TextOutputFormat.class,Text.class,Text.class);
		MultipleOutputs.addNamedOutput(job, "RareProducts",TextOutputFormat.class,Text.class,Text.class);
		MultipleOutputs.addNamedOutput(job, "OnDemandCrawlProducts", TextOutputFormat.class,Text.class,Text.class);
		MultipleOutputs.addNamedOutput(job, "AvailableProducts",TextOutputFormat.class,Text.class,Text.class);
		MultipleOutputs.addNamedOutput(job, "OtherProducts",TextOutputFormat.class,Text.class,Text.class);
		
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		System.exit(job.waitForCompletion(true)? 0:1);
		
		

			
		
	}
}

