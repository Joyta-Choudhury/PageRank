//jchoud1@uncc.edu
//Joyta Choudhury

package com.joyta;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

//Mapper for cleanup and sorting
public class CleanSortMap extends Mapper<LongWritable, Text, DoubleWritable, Text> {
	
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		try {
			
			
			String[] title_rank_links = value.toString().split("!@!");
			String[] title_rank = title_rank_links[0].split("\\|\\|");
			DoubleWritable page_rank = new DoubleWritable((Double.parseDouble(title_rank[1])));
			
			if(title_rank_links[1].contains("#####") && value.toString().length() > 1){
				
				// pagerank is key and filename is value
				context.write(page_rank, new Text(title_rank[0]));
				
			}
		} catch (Exception e) {
			
			System.out.println( e);
			
		}
	}
}