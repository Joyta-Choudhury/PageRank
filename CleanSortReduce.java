//jchoudh1@uncc.edu
//Joyta Choudhury

package com.joyta;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

//Reducer for cleanup and sorting
public class CleanSortReduce extends Reducer<DoubleWritable, Text, Text, Text> {
	
	protected void reduce(DoubleWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		for (Text text : values) {
			context.write(text, new Text(""+key));// reads only top 100 pagerank pages
		}
	}
}