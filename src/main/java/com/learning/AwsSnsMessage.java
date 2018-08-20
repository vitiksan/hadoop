package com.learning;

import com.amazonaws.services.sns.AmazonSNSClient;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class AwsSnsMessage {
    private AmazonSNSClient snsClient;

    public AwsSnsMessage() {
        this.snsClient = new AmazonSNSClient();
    }

    public void sendMessage(String message) throws IOException {
        InputStream inputStream = WordCount.class.getClassLoader().getResourceAsStream("./AwsCredentials.properties");
        Properties aws = new Properties();
        aws.load(inputStream);
        snsClient.publish(aws.getProperty("topicArn"), message);
    }
}
