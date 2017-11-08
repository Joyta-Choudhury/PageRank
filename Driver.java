//Joyta Choudhury
//jchoudh1@uncc.edu

package com.joyta;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.StringTokenizer;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class Driver extends Configured implements Tool {
	
	// main method calling series of driver methods
	public static void main(String[] args) throws Exception, IOException, InterruptedException, ClassNotFoundException {
		int res  = ToolRunner .run( new Driver(), args);
		  System .exit(res);
	}
	   
	public int run( String[] args) throws  Exception {
		
		// InvIn is the job for calculation of Inverted Index
				Job InvIn = Job.getInstance(getConf(), "invertedindex");
				InvIn.setJarByClass(this.getClass());
				FileInputFormat.addInputPaths(InvIn, args[0]);
				FileOutputFormat.setOutputPath(InvIn, new Path("InvertedIndexOutput"));
				InvIn.setMapperClass(InvertedIndexMap.class);
				InvIn.setReducerClass(InvertedIndexReduce.class);
				InvIn.setOutputKeyClass(Text.class);
				InvIn.setOutputValueClass(Text.class);
				InvIn.waitForCompletion(true);
				
		
		// count_n is the job to calculate the value of N
		FileSystem fs = FileSystem.get(getConf());
		Job count_n  = Job.getInstance(getConf(), "valueofn");
		count_n.setJarByClass( this.getClass());
		FileInputFormat.addInputPaths(count_n,  args[0]);
		FileOutputFormat.setOutputPath(count_n,  new Path("Node"));
		count_n.setMapperClass( MapCount.class);
		count_n.setReducerClass( ReduceCount.class);
		count_n.setMapOutputValueClass(IntWritable.class);
		count_n.setOutputKeyClass( Text.class);
		count_n.setOutputValueClass(IntWritable.class);
		count_n.waitForCompletion(true);
		fs.delete(new Path("Node"), true);
		
		// N value is calculated
		long value_n;
		value_n = count_n.getCounters().findCounter("Outputn", "Outputn").getValue();
		
		// job_parse is the job for parsing the file 
		Job job_parse = Job.getInstance(getConf(), "Parsing");
		job_parse.setJarByClass(this.getClass());
		job_parse.getConfiguration().setStrings("VALUE_OF_N", value_n + "");
		job_parse.setMapperClass(FirstParseMap.class);
		job_parse.setReducerClass(FirstParseReduce.class);
		job_parse.setInputFormatClass(TextInputFormat.class);
		job_parse.setOutputFormatClass(TextOutputFormat.class);
		job_parse.setOutputKeyClass(Text.class);
		job_parse.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job_parse, new Path(args[0]));
		FileOutputFormat.setOutputPath(job_parse, new Path(args[1]+"Job0"));
		
		
		job_parse.waitForCompletion(true);
		

		// job_pagerank is the job for calculating pagerank iteratively
		int i = 0;
		for(i=0 ; i < 10 ; i++) {
			
			Job job_pagerank = Job.getInstance(getConf(), "PageRank");
			job_pagerank.setJarByClass(this.getClass());
			job_pagerank.setMapperClass(PageRankMap.class);
			job_pagerank.setReducerClass(PageRankReduce.class);
			job_pagerank.setOutputKeyClass(Text.class);
			job_pagerank.setOutputValueClass(Text.class);
			job_pagerank.setInputFormatClass(TextInputFormat.class);
			job_pagerank.setOutputFormatClass(TextOutputFormat.class);
			
			FileInputFormat.setInputPaths(job_pagerank, args[1]+"Job"+i);
			FileOutputFormat.setOutputPath(job_pagerank, new Path(args[1]+"Job"+(i + 1)));
			job_pagerank.waitForCompletion(true);
			
			//deleting intermediate files
			FileSystem fs1 = FileSystem.get(getConf());
			fs1.delete(new Path(args[1]+"Job"+i), true); 
			
			
		}

		// job_cleanup is the job for cleanup and sorting
		Job job_cleanup = Job.getInstance(getConf(), "CleanAndSort");
		job_cleanup.setJarByClass(this.getClass());
		job_cleanup.setMapperClass(CleanSortMap.class);
		job_cleanup.setReducerClass(CleanSortReduce.class);
		job_cleanup.setNumReduceTasks(1);//to set the reducer to 1
		job_cleanup.setMapOutputValueClass(Text.class);
		job_cleanup.setMapOutputKeyClass(DoubleWritable.class);
		job_cleanup.setOutputKeyClass(Text.class);
		job_cleanup.setOutputValueClass(Text.class);
		job_cleanup.setInputFormatClass(TextInputFormat.class);
		job_cleanup.setOutputFormatClass(TextOutputFormat.class);
		
		job_cleanup.setSortComparatorClass(DoubleComparator.class);//Define the comparator that controls how the keys are sorted before they are passed to the Reducer.
		
		FileInputFormat.setInputPaths(job_cleanup, args[1]+ "Job"+ i);
		FileOutputFormat.setOutputPath(job_cleanup, new Path("Final_Output")); //creates Final_Output
		
		
		return job_cleanup.waitForCompletion(true) ? 0 : 1;


	}
	public static class DoubleComparator extends WritableComparator {

		public DoubleComparator() {
			super(DoubleWritable.class);
		}
		
		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			
			Double value1 = ByteBuffer.wrap(b1, s1, l1).getDouble();
			Double value2 = ByteBuffer.wrap(b2, s2, l2).getDouble();
			
			//used to sort the values in descending order instead of ascending, multiplying it by -1
			return value1.compareTo(value2) * (-1);   
		}
	}
}