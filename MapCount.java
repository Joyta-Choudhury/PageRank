//jchoudh1@uncc.edu
//Joyta Choudhury

package com.joyta;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

//Mapper used to count value of n
public class MapCount extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private static final Pattern title = Pattern.compile("<title>(.*?)</title>");

	public void map(LongWritable key, Text values, Context context)
			throws IOException, InterruptedException {

		String line_text = values.toString();
		if (line_text != null && !line_text.isEmpty()) {
			Text link = new Text();

			Matcher matcher = title.matcher(line_text);

			if (matcher.find()) {
				link = new Text(matcher.group(1));
				context.write(new Text(link), new IntWritable(1));
			}

		}

	}
}
