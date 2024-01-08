package emse.aws;

import software.amazon.awssdk.regions.Region;

public class ShopInventory {
    public static Region region = Region.US_EAST_1;
    public static String bucket = "cloudtpgp-mse" ;
    public static String topicARN = "arn:aws:sns:us-east-1:981161034142:tpSNS" ;
    public static String sqsURL = "https://sqs.us-east-1.amazonaws.com/346220435243/messaging-app-queue" ;
    public static String salesData = "src/emse/aws/sales_data/" ;
}
