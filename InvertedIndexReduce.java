package com.joyta;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

//Reducer for inverted index calculation
public class InvertedIndexReduce extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		StringBuffer sb = new StringBuffer();
		
		for (Text value : values) {
				sb.append("###" + value);
			}
		
		// extracting the values by matching the delimiters and creating a String array so that it can be sorted
		String[] temp_array = sb.toString().split("###");

		Arrays.sort(temp_array);
		StringBuffer sb2 = new StringBuffer();
		

		for (int i = 0; i < temp_array.length; i++) {
				sb2.append("###" + temp_array[i]);
			}

		// convert the urls to string
		context.write(key, new Text(sb2.toString()));
	}
}
		