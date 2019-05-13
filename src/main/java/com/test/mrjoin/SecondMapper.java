package com.test.mrjoin;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

import org.apache.hadoop.io.*;

public class SecondMapper extends Mapper<LongWritable,Text,Text,Text> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
	String record = value.toString();
	String[] parts = record.split(",");
	context.write(new Text(parts[0]), new Text("second\t" + parts[1]));
	}

}
