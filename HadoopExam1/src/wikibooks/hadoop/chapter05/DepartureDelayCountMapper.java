package wikibooks.hadoop.chapter05;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class DepartureDelayCountMapper extends Mapper<LongWritable , Text, Text,
IntWritable > {
	
	private final static IntWritable outputValue = new IntWritable(1);
	private Text outputKey = new Text();
	
	public void map(LongWritable key, Text value , Context context)
	throws IOException , InterruptedException
	{
		if (key.get() > 0 )
		{
			String[] columns = value.toString().split(",");
			if (columns != null && columns.length > 0)
			{
				try
				{
					outputKey.set(columns[0]+","+columns[1]);
					if (!columns[15].equals("NA")) {
						int depDelayTime = Integer.parseInt(columns[15]);
						if(depDelayTime > 0 )
						{
							outputValue.set(depDelayTime);;
							context.write(outputKey, outputValue);
						}
					}
					
				} catch(Exception e)
				{
					e.printStackTrace();;
				}
			}
		}
	}
	
}
