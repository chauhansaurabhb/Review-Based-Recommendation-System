package com.textminer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class TextDriver extends Configured implements Tool{

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TextDriver(), args);
        System.exit(res);       
    }

    @Override
    public int run(String[] args) throws Exception {
            	
    	// JobConf represents a MapReduce job configuration.

    	//JobConf is the primary interface for a user to describe a MapReduce job to the Hadoop 
    	//framework for execution. The framework tries to faithfully execute the job as described by JobConf, however:
    	Configuration conf = new Configuration();

    	Job job = new Job(conf, "Votcount");

    	

//configure the input output key and value class
    	job.setOutputKeyClass(Text.class);

    	job.setOutputValueClass(IntWritable.class);

    	job.setMapperClass(TextMapper.class);
        job.setReducerClass(TextReducer.class);

    	job.setInputFormatClass(TextInputFormat.class);

    	job.setOutputFormatClass(TextOutputFormat.class);
    	
// add the input and out directory path
    	//FileInputFormat.addInputPath(job, new Path(("/home/shree/hadoop/workspace/TextInput-Samsung")));
    	FileInputFormat.addInputPath(job, new Path("/home/shree/hadoop/workspace/TextInput-Iphone/"));
    	//FileInputFormat.addInputPath(job, new Path("/home/shree/hadoop/workspace/TextOutput-Samsung/"));
       	FileOutputFormat.setOutputPath(job, new Path("/home/shree/hadoop/workspace/TextOutput-Iphone/"));

    	job.waitForCompletion(true);
    	return 0;
    	}
    	}