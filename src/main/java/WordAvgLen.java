import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordAvgLen
{
	
    public static class TokenizerMapper
       extends Mapper<LongWritable, Text, IntWritable, SumAndCount>
    {

        private final static IntWritable one = new IntWritable(1);

        public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException
        {
        	String st [] = value.toString().split("\\s+");
    		
    		
            for(String word :  st) {
         
                String wordnew=word.replaceAll("[^a-zA-Z]",""); 
                
                if(!wordnew.isEmpty()){
                	
                	context.write(one,new SumAndCount(wordnew.length(),1));
                	
                }
                else continue;
            	
 
            }
        }
    }
   
    
    public static class IntSummer
       extends Reducer<IntWritable,SumAndCount,IntWritable,SumAndCount>
    {
    	private final static IntWritable one = new IntWritable(1);
    	//private SumAndCount sumandcount = new SumAndCount(0,0);
    	public void reduce(IntWritable key, Iterable<SumAndCount> values, Context context)
    	throws IOException, InterruptedException
    	{
    		int sum=0, count=0;
    		
    		
    		for(SumAndCount val : values)
    		{
    			count+=val.getCount();
    			sum+=val.getSum();
    		}
    		
    		//sumandcount.setSum(sum);
    		//sumandcount.setCount(count);
    		context.write(one, new SumAndCount(sum, count));
    		
    	}
    }

    public static class IntSumReducer
       extends Reducer<IntWritable,SumAndCount,Text,Text>
    {
        public void reduce(IntWritable key, Iterable<SumAndCount> values,
                       Context context
                       ) throws IOException, InterruptedException
        {
            int sum=0,count=0;

            for (SumAndCount val : values)
            {
                sum += val.getSum();
                count+=val.getCount();
            }

            float avg=(sum/(float)count);
            String op="Average length of " + count + " words = " + avg;
            context.write(new Text(op), new Text(""));

        }
    }




    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "wordLenAvgCombiner");

        job.setJarByClass(WordAvgLen.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSummer.class);
        job.setReducerClass(IntSumReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(SumAndCount.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
