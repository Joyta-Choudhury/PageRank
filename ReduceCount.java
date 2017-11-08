//jchoudh1@uncc.edu
//Joyta Choudhury

package com.joyta;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

//Reducer to count the values of n
public class ReduceCount extends Reducer<Text, IntWritable, Text, IntWritable> {

	int counter = 0;
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		this.counter++;
	}

	protected void cleanup(Context context) throws IOException,
	InterruptedException {
		context.getCounter("Outputn", "Outputn").increment(counter);
	}
}