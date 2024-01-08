package emse.aws;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;
import software.amazon.awssdk.services.sns.model.SnsException;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

import java.io.File;
import java.nio.file.Path;

public class Client {

    public static void main(String[] args) {
        String storeName = "store1";
        String date = "01-10-2022";
        String lambdaOrJavaApp= "Java application";
//        String lambdaOrJavaApp= "lambda";
        String fileName = date + "-" + storeName + ".csv";

        File file = new File(ShopInventory.salesData + fileName);

        S3Client s3 = S3Client.builder().region(ShopInventory.region).build();
        try {
            boolean bucketExists = Bucket.bucketCheck(s3, ShopInventory.bucket);
            System.out.println("Bucket Exists: " + bucketExists);
            System.exit(1);

            if (!bucketExists) {
                Bucket.createBucket(s3, ShopInventory.bucket);
                System.out.println("Bucket:" + ShopInventory.bucket + " created");
                bucketExists = true;
            }

            if (bucketExists) {
                Path filePath = file.toPath();

                boolean fileUploaded = Bucket.uploadBucket(s3, ShopInventory.bucket, fileName,
                        filePath);

                if (fileUploaded) {
                    System.out.println("file uploaded");

                    if (lambdaOrJavaApp.equals("lambda")) {
                        System.out.println("Lambda will be used");
                        try {
                            SnsClient snsClient = SnsClient.builder().region(ShopInventory.region).build();
                            PublishRequest request = PublishRequest.builder()
                                    .message(storeName + ";" + ShopInventory.bucket + ";" + fileName)
                                    .topicArn(ShopInventory.topicARN).build();
                            PublishResponse snsResponse = snsClient.publish(request);
                            System.out.println(snsResponse.messageId() + " Notification sent to worker. Status is "
                                    + snsResponse.sdkHttpResponse().statusCode());
                        } catch (SnsException e) {
                            System.err.println(e.awsErrorDetails().errorCode());
                            System.exit(1);
                        }
                    } else if (lambdaOrJavaApp.equals("Java application")) {
                        System.out.println("EC2 will be used");
                        SqsClient sqsClient = SqsClient.builder().region(ShopInventory.region).build();
                        SendMessageRequest sendRequest = SendMessageRequest.builder().queueUrl(ShopInventory.sqsURL)
                                .messageBody(ShopInventory.bucket + ";" + fileName).build();
                        SendMessageResponse sqsResponse = sqsClient.sendMessage(sendRequest);
                        System.out.println(sqsResponse.messageId() + " Message sent. Status is "
                                + sqsResponse.sdkHttpResponse().statusCode());
                    }
                } else {
                    System.out.println("file upload failed");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

