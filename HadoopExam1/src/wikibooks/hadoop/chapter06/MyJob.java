package wikibooks.hadoop.chapter06;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.examples.SecondarySort.Reduce;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.Path;

public class MyJob extends Configured implements Tool {
	
	public static class MapClass extends MapReduceBase implements Mapper<Text , Text , Text , Text>
	{
		
		public void map(Text key, Text value, OutputCollector<Text,Text> output, Reporter reporter) 
		throws IOException
		{
			output.collect(value, key);
		}
		
	}
	
	public static class ReduceClass extends MapReduceBase implements Reducer<Text,Text,Text,IntWritable>
	{
		public void reduce(Text key , Iterator<Text> values , OutputCollector<Text,IntWritable> output,Reporter
				reporter) throws IOException
		{
			int count = 0;
			while(values.hasNext()){
				values.next();
				count++;
			}
			
			output.collect(key, new IntWritable(count));
		}
	}
	
	public int run(String[] args) throws Exception
	{
		Configuration conf = getConf();
		JobConf job = new JobConf(conf,MyJob.class);
		
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		
		job.setJobName("MyJob");
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
		
		job.setInputFormat(KeyValueTextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.set("key.value.seperator.in.input.line", ","); 
		JobClient.runJob(job);
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception
	{
		int res = ToolRunner.run(new Configuration(),  new MyJob(),args);
		System.exit(res);
	}

}
