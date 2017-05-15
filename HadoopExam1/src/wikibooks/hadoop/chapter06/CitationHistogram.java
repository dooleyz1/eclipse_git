package wikibooks.hadoop.chapter06;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;

public class CitationHistogram extends Configured implements Tool {
	
	public static class MapperClass extends MapReduceBase implements Mapper<Text,Text,Text,IntWritable>
	{
		private final static IntWritable uno = new IntWritable();
		private IntWritable citationCount = new IntWritable();
		
	}

}
