package com.test.textpair;

import java.io.IOException;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;

public class WordcoDriver extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
        int returnStatus = ToolRunner.run(new Configuration(), new WordcoDriver(), args);
        System.exit(returnStatus);
    }


public int run(String[] args) throws IOException{



	Job job = new Job(getConf());
	
	
	 job.setJobName("Word Cooccurence");
	
	 job.setJarByClass(WordcoDriver.class);

	 job.setOutputKeyClass(TextPair.class);
	 job.setOutputValueClass(IntWritable.class);
	 job.setMapOutputKeyClass(TextPair.class);
	 job.setMapOutputValueClass(IntWritable.class);
	 
	 job.setMapperClass(WordcoMapper.class);
	 job.setCombinerClass(WordcoReducer.class);
	 job.setReducerClass(WordcoReducer.class);
	
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job,new Path(args[1]));
	   	
	
	try {
		return job.waitForCompletion(true) ? 0 : 1;
	} catch (ClassNotFoundException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	return 0;
	
	
  
}


}
