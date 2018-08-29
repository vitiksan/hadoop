package com.learning;

import com.amazonaws.services.sns.AmazonSNSClient;
import com.learning.wordCount.WordCountRunner;
import org.apache.hadoop.mapreduce.Job;
import org.xml.sax.helpers.DefaultHandler;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class AwsUtil extends DefaultHandler {

    private AwsUtil() {
    }

    public static void setS3FS(Job job) throws IOException {
        InputStream inputStream = AwsUtil.class.getClassLoader().getResourceAsStream("./AwsCredentials.properties");
        Properties aws = new Properties();
        aws.load(inputStream);
        job.getConfiguration().set("fs.s3n.awsAccessKeyId", aws.getProperty("accessKey"));
        job.getConfiguration().set("fs.s3n.awsSecretAccessKey", aws.getProperty("secretKey"));
        job.getConfiguration().set("fs.defaultFS", "s3n://" + aws.getProperty("bucketName"));
    }


    public static void sendMessage(String message) throws IOException {
        AmazonSNSClient snsClient = new AmazonSNSClient();
        InputStream inputStream = WordCountRunner.class.getClassLoader().getResourceAsStream("./AwsCredentials.properties");
        Properties aws = new Properties();
        aws.load(inputStream);
        snsClient.publish(aws.getProperty("topicArn"), message);
    }
}
