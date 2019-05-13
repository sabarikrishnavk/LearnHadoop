package com.test.counter;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

/**
 * Example input line:
 * 96.7.4.14 - - [24/Apr/2011:04:20:11 -0400] "GET /cat.jpg HTTP/1.1" 200 12433
 *
 */
public class CounterMapper extends
    Mapper<LongWritable, Text, Text, IntWritable> {
	enum InfoCounter{EMAIL,MOBILE};

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
   
    String[] fields = value.toString().split(",");
    if (fields.length > 1) {
      int email = Integer.parseInt(fields[10]);
      int mobile = Integer.parseInt(fields[11]);
      if (email==1) {
          context.getCounter(InfoCounter.EMAIL).increment(1);
       }
      if (mobile==1) {
    	  context.getCounter(InfoCounter.MOBILE).increment(1);
      }
    }
  }
}