package com.learning;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.CreateTopicResult;

public class AwsSnsMessage {
    private AmazonSNSClient snsClient;
    private String topicArn;

    public AwsSnsMessage() {
        AWSCredentialsProvider awsCredentialsProvider = new ClasspathPropertiesFileCredentialsProvider();
        this.snsClient = new AmazonSNSClient(new ClasspathPropertiesFileCredentialsProvider());
        this.snsClient.setRegion(Region.getRegion(Regions.US_EAST_2));
    }

    public void createTopic() {
        CreateTopicResult result = snsClient.createTopic("JobStatus");
        topicArn =  result.getTopicArn();
    }

    public void subscribe(String email){
        snsClient.subscribe(topicArn, "email", email);

    }

    public void sendMessage(String message) {
        snsClient.publish(topicArn, message);
    }

    public static void main(String[] args) {
        AwsSnsMessage snsMessage = new AwsSnsMessage();
        snsMessage.createTopic();
        snsMessage.subscribe("patriot02faqq@gmail.com");
    }
}
