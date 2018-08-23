package com.learning.wordCount;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.learning.AwsUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class WordCountRunner {

    private static Job setUp(String name, String input, String output, boolean onS3, Class<? extends Mapper> mapper,
                             Class<? extends Reducer> reducer, Class key, Class value) throws IOException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, name);
        job.setJarByClass(WordCountRunner.class);

        job.setMapperClass(mapper);
        job.setReducerClass(reducer);
        job.setCombinerClass(reducer);

        job.setOutputKeyClass(key);
        job.setOutputValueClass(value);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        if (onS3) AwsUtil.setS3FS(job);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        return job;
    }

    public static int run(String input, String output, boolean onS3) throws IOException {
        String tempDir = "tempDirForJob";

        Job wordCounter = setUp("word counter", input, tempDir, onS3, WordCountMapper.class,
                WordCountReducer.class, Text.class, IntWritable.class);

        FileSystem fs = FileSystem.get(wordCounter.getConfiguration());
        try {
            boolean isSuccessfully = wordCounter.waitForCompletion(true);
            Job sortJob = setUp("word sort", tempDir, output, onS3, WordSortMapper.class,
                    WordSortReducer.class, IntWritable.class, Text.class);
            if (!isSuccessfully) throw new AmazonS3Exception("word counter job failed");

            isSuccessfully = sortJob.waitForCompletion(true);
            if (!isSuccessfully) throw new AmazonS3Exception("word counter sort job failed");
        } catch (Exception e) {
//           AwsUtil.sendMessage("Job finished with error: " + e.getMessage() + "\n" + e.getStackTrace());
            return 1;
        } finally {
            fs.delete(new Path(tempDir));
            fs.rename(new Path(output + "/part-r-00000"), new Path(tempDir));
            fs.delete(new Path(output), true);
            fs.rename(new Path(tempDir), new Path(output));
        }
//        AwsUtil.sendMessage("Job finished successfully");
        return 0;
    }
}
