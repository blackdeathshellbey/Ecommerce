package emse.aws;

import com.amazonaws.services.lambda.runtime.Context;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.*;
import java.nio.file.Path;
import java.util.ArrayList;

import static emse.aws.Bucket.bucketCheck;
import static emse.aws.Bucket.uploadBucket;

public class Parser {
    public static void DownloadFile(String bucketName, String fileName, S3Client s3) throws FileNotFoundException {
        if (bucketCheck(s3, bucketName)) {
            GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucketName).key(fileName).build();

            ResponseInputStream<GetObjectResponse> response = s3.getObject(getObjectRequest);
            BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(fileName));
            byte[] buffer = new byte[4096];
            int bytesRead = -1;
            try {
                while ((bytesRead = response.read(buffer)) != -1) {
                    outputStream.write(buffer, 0, bytesRead);
                }
                response.close();
                outputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println(fileName + " downloaded");
        }
    }

    public static void deleteFile(String fileName) {
        File file = new File(fileName);
        file.delete();
        System.out.println("File, " + fileName + " deleted");
    }

    public static void consolidatorCSV(S3Client s3, Double[] Profit, String[] Store, ArrayList<ProductData> products,
                                       String date) throws IOException {
        FileWriter csvWriter;
        String fileName = "Consolidated-" + date.replace("-", ".") + ".csv";
        csvWriter = new FileWriter(fileName);

        csvWriter.append("Minimum earned");
        csvWriter.append(";");
        csvWriter.append(Store[0]);
        csvWriter.append(";");
        csvWriter.append(Double.toString(Profit[0]));
        csvWriter.append("\n");
        csvWriter.append("Maximum earned");
        csvWriter.append(";");
        csvWriter.append(Store[1]);
        csvWriter.append(";");
        csvWriter.append(Double.toString(Profit[1]));
        csvWriter.append("\n");
        csvWriter.append("\n");

        csvWriter.append("Product");
        csvWriter.append(";");
        csvWriter.append("TotalQuantity");
        csvWriter.append(";");
        csvWriter.append("TotalPrice");
        csvWriter.append(";");
        csvWriter.append("TotalProfit");
        csvWriter.append("\n");

        for (ProductData product : products) {
            csvWriter.append(product.getName());
            csvWriter.append(";");
            csvWriter.append(Double.toString(product.getQuantity()));
            csvWriter.append(";");
            csvWriter.append(Double.toString(product.getPrice()));
            csvWriter.append(";");
            csvWriter.append(Double.toString(product.getProfit()));
            csvWriter.append("\n");
        }

        csvWriter.flush();
        csvWriter.close();

        System.out.println("Writing done for summary file:" + fileName);
        uploadBucket(s3, fileName);
        deleteFile(fileName);
    }

    public static boolean workerCSV(double totalProfit, ArrayList<ProductData> products, String fileName, S3Client s3) throws IOException {

        FileWriter csvWriter;
        csvWriter = new FileWriter(fileName);

        csvWriter.append("Total Profit");
        csvWriter.append(";");
        csvWriter.append(Double.toString(totalProfit));
        csvWriter.append("\n");
        csvWriter.append("\n");

        csvWriter.append("Product");
        csvWriter.append(";");
        csvWriter.append("Quantity");
        csvWriter.append(";");
        csvWriter.append("Total Price");
        csvWriter.append(";");
        csvWriter.append("Total Profit");
        csvWriter.append("\n");

        for (ProductData product : products) {
            csvWriter.append(product.getName());
            csvWriter.append(";");
            csvWriter.append(Double.toString(product.getQuantity()));
            csvWriter.append(";");
            csvWriter.append(Double.toString(product.getPrice()));
            csvWriter.append(";");
            csvWriter.append(Double.toString(product.getProfit()));
            csvWriter.append("\n");
        }

        csvWriter.flush();
        csvWriter.close();

        File file = new File(fileName);
        Path filePath = file.toPath();

        System.out.println("new file: " + filePath);

        return true;
    }
    public static boolean lambdaWorkerCSV(double totalProfit, ArrayList<ProductData> products, String fileName, S3Client s3, Context context) throws Exception {

        String initialFileName = fileName;
        fileName = "/tmp/"+fileName;

        FileWriter csvWriter;
        try {
            csvWriter = new FileWriter(fileName);

            csvWriter.append("Total Profit");
            csvWriter.append(";");
            csvWriter.append(Double.toString(totalProfit));
            csvWriter.append("\n");
            csvWriter.append("\n");

            csvWriter.append("Product");
            csvWriter.append(";");
            csvWriter.append("TotalQuantity");
            csvWriter.append(";");
            csvWriter.append("TotalPrice");
            csvWriter.append(";");
            csvWriter.append("TotalProfit");
            csvWriter.append("\n");

            for (ProductData product : products) {
                csvWriter.append(product.getName());
                csvWriter.append(";");
                csvWriter.append(Double.toString(product.getQuantity()));
                csvWriter.append(";");
                csvWriter.append(Double.toString(product.getPrice()));
                csvWriter.append(";");
                csvWriter.append(Double.toString(product.getProfit()));
                csvWriter.append("\n");
            }

            csvWriter.flush();
            csvWriter.close();
            File file = new File(fileName);
            context.getLogger().log("Writing done for summary file:" + fileName );
            Path filePath = file.toPath();
            context.getLogger().log("Path:" + filePath );
            boolean bucketUploaded = Bucket.uploadBucket(s3, ShopInventory.bucket, initialFileName, filePath);

            if(bucketUploaded) {
                System.out.println("file uploaded");
                context.getLogger().log("Summary file uploaded to :" + ShopInventory.bucket );
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }

}
