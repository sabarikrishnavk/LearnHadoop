package com.test.mrjoin;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;

public class MrjoinReducer extends Reducer<Text,Text,Text,Text> {
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException 
			{
			List<String>users = new ArrayList<String>();
			List<String>products = new ArrayList<String>();
			for (Text t : values) 
			{ 
			String parts[] = t.toString().split("\t");
			if (parts[0].equals("first")) 
			{
			users.add(parts[1]);
			} 
			else if (parts[0].equals("second")) 
			{
			 products.add(parts[1]);
			}
			}
			for(String user:users) {
				for(String product:products) {
					String str = key+"\t"+product;
					context.write(new Text(user), new Text(str));
				}
			}
			
			}

}
