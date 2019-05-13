package com.test.mrjoin;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

import org.apache.hadoop.io.*;

public class FirstMapper extends Mapper<LongWritable,Text,Text,Text>{
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{String record = value.toString();
	String[] parts = record.split(",");
	context.write(new Text(parts[1]), new Text("first\t" + parts[0]));
		
		
		
	
	}
}
