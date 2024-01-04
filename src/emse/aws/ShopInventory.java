package emse.aws;

import software.amazon.awssdk.regions.Region;

public class ShopInventory {
    public static Region region = Region.US_EAST_1;
    public static String bucket = "" ;
    public static String topicARN = "" ;
    public static String sqsURL = "" ;
    public static String salesData = "src/emse/aws/sales_data" ;
}
