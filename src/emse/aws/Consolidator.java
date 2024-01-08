package emse.aws;

import java.io.*;
import java.util.*;
import java.util.stream.IntStream;

import software.amazon.awssdk.services.s3.*;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

import static emse.aws.Bucket.bucketCheck;
import static emse.aws.Bucket.createBucket;
import static emse.aws.Parser.*;

public class Consolidator {
    public static void main(String[] args) throws IOException {
        Date executionDate = new Date();
        System.out.println(executionDate);
        S3Client s3 = S3Client.builder().region(ShopInventory.region).build();
//        String date = args[0].toString();
        String date = "01-10-2022";
        Double[] Profit = new Double[2];
        String[] Store = new String[2];
        ArrayList<ProductData> products = new ArrayList<ProductData>();
        if (!bucketCheck(s3, ShopInventory.bucket)) {
            createBucket(s3, ShopInventory.bucket);
        }
        List<String> fileNames = filesManagement(s3, ShopInventory.bucket, date);
        int fileNumber = 0;
        for (String fileName : fileNames) {
            String storeName = fileName.substring(fileName.indexOf(date) + 11, fileName.length() - 4);
            try {
                DownloadFile(ShopInventory.bucket, fileName, s3);
            } catch (IOException e) {
                e.printStackTrace();
            }

            BufferedReader reader;
            int count = 0;
            try {
                reader = new BufferedReader(new FileReader(fileName));
                String line = reader.readLine();
                while (line != null) {
                    if (count == 0) {
                        String[] cols = line.split(";");
                        Double totalProfit = Double.parseDouble(cols[1]);
                        if (fileNumber == 0) {
                            Profit[0] = totalProfit;
                            Profit[1] = totalProfit;
                            Store[0] = storeName;
                            Store[1] = storeName;
                        } else {
                            if (totalProfit < Profit[0]) {
                                Profit[0] = totalProfit;
                                Store[0] = storeName;
                            }
                            if (totalProfit > Profit[0]) {
                                Profit[1] = totalProfit;
                                Store[1] = storeName;
                            }
                        }
                    } else if (count == 1 || count == 2) {

                    } else {
                        String[] cols = line.split(";");
                        String nameToMatch = cols[0];

                        try {
                            int index = IntStream.range(0, products.size())
                                    .filter(i -> nameToMatch.equals(products.get(i).getName())).findFirst().getAsInt();
                            ProductData existingProduct = products.get(index);

                            ProductData newProduct = new ProductData(cols[0], Double.parseDouble(cols[1]),
                                    Double.parseDouble(cols[2]), Double.parseDouble(cols[3]));

                            existingProduct.incrementAll(newProduct);
                            products.set(index, existingProduct);

                        } catch (NoSuchElementException e) {
                            ProductData newProduct = new ProductData(cols[0], Double.parseDouble(cols[1]),
                                    Double.parseDouble(cols[2]), Double.parseDouble(cols[3]));
                            products.add(newProduct);

                        }
                    }
                    count++;
                    line = reader.readLine();
                }
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            fileNumber++;
            deleteFile(fileName);
        }
        consolidatorCSV(s3, Profit, Store, products, date);

    }
    public static List<String> filesManagement(S3Client client, String bucketName, String date) {
        List<String> files = new ArrayList<>();
        try {
            ListObjectsRequest listObjects = ListObjectsRequest.builder().bucket(bucketName).build();
            ListObjectsResponse res = client.listObjects(listObjects);
            List<S3Object> objects = res.contents();
            for (S3Object obj : objects) {
                if (obj.key().contains(date)) {
                    files.add(obj.key());
                }
            }
            return files;
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
            return null;
        }
    }
}
