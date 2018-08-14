package com.learning;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class WordCount {

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.out.println("Error! False count of arguments!");
            return;
        }

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "word counter");
        job.setJarByClass(WordCount.class);

        job.setMapperClass(WordMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if (job.isSuccessful()) System.out.println("Job successfully completed");
        else if (job.isComplete()) System.out.println("Job completed with not expected result");
        else System.out.println("Job failed");
    }
}
