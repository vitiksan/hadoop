package com.learning;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class WordCount {

    private static void setS3FS(Job job) throws IOException {
        InputStream inputStream = WordCount.class.getClassLoader().getResourceAsStream("./AwsCredentials.properties");
        Properties aws = new Properties();
        aws.load(inputStream);
        job.getConfiguration().set("fs.s3n.awsAccessKeyId", aws.getProperty("accessKey"));
        job.getConfiguration().set("fs.s3n.awsSecretAccessKey", aws.getProperty("secretKey"));
        job.getConfiguration().set("fs.defaultFS", "s3n://" + aws.getProperty("bucketName"));
    }


    private static Job setUp(String name, String input, String output, boolean onS3, Class<? extends Mapper> mapper,
                             Class<? extends Reducer> reducer, Class key, Class value) throws IOException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, name);
        job.setJarByClass(WordCount.class);

        job.setMapperClass(mapper);
        job.setReducerClass(reducer);
        job.setCombinerClass(reducer);

        job.setOutputKeyClass(key);
        job.setOutputValueClass(value);
        job.setOutputFormatClass(TextOutputFormat.class);

        if (onS3) setS3FS(job);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        return job;
    }

    private static int run(String input, String output, String tempDir, boolean onS3) throws IOException {
        Job wordCounter = setUp("word counter", input, tempDir, onS3, WordMapper.class,
                WordCountReducer.class, Text.class, IntWritable.class);

        try {
            boolean isSuccessfully = wordCounter.waitForCompletion(true);
            Job sortJob = setUp("word sort", tempDir, output, onS3, WordSortMapper.class,
                    WordSortReducer.class, IntWritable.class, Text.class);
            if (!isSuccessfully) throw new AmazonS3Exception("word counter job failed");

            isSuccessfully = sortJob.waitForCompletion(true);
            if (!isSuccessfully) throw new AmazonS3Exception("word counter sort job failed");

            FileSystem fs = FileSystem.get(sortJob.getConfiguration());
            fs.delete(new Path(tempDir));
        } catch (Exception e) {
//            new AwsSnsMessage().sendMessage("Job finished with error: " + e.getMessage() + "\n" + e.getStackTrace());
            return 1;
        }
//        new AwsSnsMessage().sendMessage("Job finished successfully");
        return 0;
    }

    public static void main(String[] args) throws Exception {
        boolean onS3 = args[0].equals("s3");
        String input = "inputPath";
        String output = "outputPath";
        String tempDir = "tempDirForJob";

        if (onS3 && args.length == 3) {
            input = args[1];
            output = args[2];
        } else if (!onS3 && args.length == 2) {
            input = args[0];
            output = args[1];
        } else {
            System.out.println("Error! False list of arguments!");
            System.exit(2);
        }

        System.exit(run(input, output, tempDir, onS3));
    }
}
