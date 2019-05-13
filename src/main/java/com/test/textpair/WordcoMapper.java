package com.test.textpair;
import org.apache.hadoop.mapreduce.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.*;

public class WordcoMapper extends Mapper<LongWritable, Text, TextPair, IntWritable> {
    private TextPair wordPair = new TextPair();
    private IntWritable ONE = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        int neighbors = context.getConfiguration().getInt("neighbors", 2);
        String[] tokens = value.toString().split("\\s+");
        if (tokens.length > 1) {
          for (int i = 0; i < tokens.length; i++) {
              wordPair.setFirst(new Text(tokens[i]));

             int start = (i - neighbors < 0) ? 0 : i - neighbors;
             int end = (i + neighbors >= tokens.length) ? tokens.length - 1 : i + neighbors;
              for (int j = start; j <= end; j++) {
                  if (j == i) continue;
                   wordPair.setSecond(new Text(tokens[j]));
                   context.write(wordPair, ONE);
              }
          }
      }
  }
}

class TextPair implements WritableComparable<TextPair> {
	 
    private Text first;
    private Text second;
 
    public TextPair(Text first, Text second) {
        setFirst(first);
        setSecond(second);
    }
 
    public TextPair() {
        setFirst(new Text());
        setSecond(new Text());
    }
 
    public TextPair(String first, String second) {
        setFirst(new Text(first));
        setSecond(new Text(second));
    }
 
    public Text getFirst() {
        return first;
    }
 
    public Text getSecond() {
        return second;
    }
 
    public void setFirst(Text first) {
        this.first = first;
    }
    
    public void setSecond(Text second) {
    	this.second=second;
    }
 
    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
    }
 
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
    }
 
    @Override
    public String toString() {
        return first + " " + second;
    }
 
    public int compareTo(TextPair tp) {
        int cmp = first.compareTo(tp.first);
 
        if (cmp != 0) {
            return cmp;
        }
 
        return second.compareTo(tp.second);
    }
 
    @Override
    public int hashCode(){
        return first.hashCode()*163 + second.hashCode();
    }
 
    @Override
    public boolean equals(Object o)
    {
        if(o instanceof TextPair)
        {
            TextPair tp = (TextPair) o;
            return first.equals(tp.first) && second.equals(tp.second);
        }
        return false;
    }

    
 
}

