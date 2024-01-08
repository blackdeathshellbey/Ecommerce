package emse.aws;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;

import java.io.File;
import java.nio.file.Path;

import static emse.aws.Parser.deleteFile;

public class Bucket {

    public static boolean uploadBucket(S3Client client, String bucketName, String bucketKey, Path filePath) throws Exception {
        client.putObject(PutObjectRequest.builder().bucket(bucketName).key(bucketKey)
                .build(), filePath);
        return true;
    }

    ////
    public static void createBucket(S3Client client, String bucketName) {
        try{
            S3Waiter s3Waiter = client.waiter();
            CreateBucketRequest bucketRequest = CreateBucketRequest.builder().bucket(bucketName).build();

            client.createBucket(bucketRequest);
            HeadBucketRequest bucketRequestWait = HeadBucketRequest.builder().bucket(bucketName).build();

            WaiterResponse<HeadBucketResponse> waiterResponse = s3Waiter.waitUntilBucketExists(bucketRequestWait);
            waiterResponse.matched().response().ifPresent(System.out::println);
            System.out.println("ready to use " + bucketName);
        }catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }

    }

    static boolean bucketCheck(S3Client client, String bucketName) {
        ListBucketsRequest listBucketsRequest = ListBucketsRequest.builder().build();
        ListBucketsResponse listBucketResponse = client.listBuckets(listBucketsRequest);
        return listBucketResponse.buckets().stream().anyMatch(x -> x.name().equals(bucketName));
    }

    public static void uploadBucket(S3Client client, String fileName) {
        PutObjectRequest request = PutObjectRequest.builder().bucket(ShopInventory.bucket).key(fileName).build();
        client.putObject(request, RequestBody.fromFile(new File(fileName)));
        System.out.println("File updated: " + fileName);
        deleteFile(fileName);
    }

}
