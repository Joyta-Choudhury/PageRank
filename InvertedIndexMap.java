package com.joyta;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


// Mapper for Inverted Index calculation
public class InvertedIndexMap extends Mapper<Object, Text, Text, Text> {

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		String line_text = value.toString();
		Pattern pattern;
		Matcher matcher;
		
		try {

			// create postings list for each node by pattern matching
			if (line_text != null && !line_text.trim().equals("")) {

				// matches pattern for the title
				Pattern title_pat = Pattern.compile("<title>(.*?)</title>");
				Matcher title_mat = title_pat.matcher(line_text);
				title_mat.find();
				String title = title_mat.group(1);

				// matches pattern for the nodes in text
				Pattern text_pat = Pattern.compile("<text>(.*?)</text>");
				Matcher text_mat = text_pat.matcher(line_text);
				text_mat.find();
				String text_tags = text_mat.group(1);

				pattern = Pattern.compile("\\[\\[.*?]\\]");
				matcher = pattern.matcher(text_tags);

				while (matcher.find()) {

					String links = matcher.group().replace("[[", "").replace("]]", "");
					if (!links.isEmpty()) {

						// swapping and using links as key and page title as value to the reducer.
						//checking inlinks to every node present
						context.write(new Text(links), new Text(title.trim()));
					}
				}

			}
		} 
		
		catch (Exception e) {
			return;
		}
		
		
	}
}
