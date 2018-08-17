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
import java.io.InputStream;
import java.util.Properties;
import java.util.Scanner;

public class WordCount {

    private static Job setUp(String input, String output, boolean onS3) throws IOException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "word counter");
        job.setJarByClass(WordCount.class);

        job.setMapperClass(WordMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setCombinerClass(WordCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        if (onS3) {
            InputStream inputStream = WordCount.class.getClassLoader().getResourceAsStream("AwsCredentials.properties");
            Properties aws = new Properties();
            aws.load(inputStream);
            job.getConfiguration().set("fs.s3n.awsAccessKeyId", aws.getProperty("accessKey"));
            job.getConfiguration().set("fs.s3n.awsSecretAccessKey", aws.getProperty("secretKey"));
            job.getConfiguration().set("fs.defaultFS", "s3n://" + aws.getProperty("bucketName"));
        }

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        return job;
    }

    public static void main(String[] args) throws Exception {
        boolean onS3 = args[0].equals("s3");
        String inputPath;
        String outputPath;
        String email;

        if (onS3 && args.length == 4) {
            inputPath = args[1];
            outputPath = args[2];
            email = args[3];
        } else if (!onS3 && args.length == 3) {
            inputPath = args[0];
            outputPath = args[1];
            email = args[2];
        } else {
            System.out.println("Error! False list of arguments!");
            return;
        }

        /*AwsSnsMessage snsMessage = new AwsSnsMessage();
        snsMessage.createTopic();
        snsMessage.subscribe(email);
        */System.out.println("Email for subscribe on this job has sent.");

        String temp;
        do {
            System.out.println("Confirm it and print : Yes (y) or Not (n) if you did not receive email");
            Scanner in = new Scanner(System.in);
            temp = in.next();
            if (temp.equalsIgnoreCase("n")) {
                System.out.print("Re-enter email: ");
                email = in.next();
//                snsMessage.subscribe(email);
                System.out.println("Message has sent again");
            }
        } while (!temp.equalsIgnoreCase("y"));


        Job job = setUp(inputPath, outputPath, onS3);


        boolean isSuccessfully = job.waitForCompletion(false);
        /*if (isSuccessfully)
            snsMessage.sendMessage("Job finished successfully");
        else
            snsMessage.sendMessage("Job finished with error");
        */System.exit(isSuccessfully ? 0 : 1);
    }
}
