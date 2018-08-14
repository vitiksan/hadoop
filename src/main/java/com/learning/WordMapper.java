package com.learning;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordMapper extends Mapper<NullWritable, Text, Text, IntWritable> {
    private final static IntWritable ONE = new IntWritable(1);

    @Override
    protected void map(NullWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split("\\p{Space}|\\p{Punct}");

        for (String word : words) {
            context.write(new Text(word), ONE);
        }
    }
}