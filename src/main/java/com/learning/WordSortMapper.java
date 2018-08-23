package com.learning;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordSortMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer tokenizer = new StringTokenizer(value.toString());

        String word = "nothing";
        int count = 0;

        if (tokenizer.hasMoreTokens())
            word = tokenizer.nextToken().trim();
        if (tokenizer.hasMoreTokens())
            count = Integer.parseInt(tokenizer.nextToken().trim());

        context.write(new IntWritable(count), new Text(word));
    }
}
