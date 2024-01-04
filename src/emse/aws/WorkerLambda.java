package emse.aws;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.NoSuchElementException;
import java.util.stream.IntStream;

public class WorkerLambda implements RequestHandler <SNSEvent, Object> {

    public Object handleRequest(SNSEvent request, Context context) {

        String timeStamp = new SimpleDateFormat("yyy-MM-dd_HH:mm:ss").format(Calendar.getInstance().getTime());
        context.getLogger().log("Invocation started: " + timeStamp);
        String message = request.getRecords().get(0).getSNS().getMessage();
        context.getLogger().log(message);

        if (message != null) {
            S3Client client = S3Client.builder().region(ShopInventory.region).build();

            String[] messageContent = message.split(";");
            String bucketName = messageContent[1];
            String fileKey = messageContent[2].trim();

            AmazonS3 amazonClient = AmazonS3ClientBuilder.defaultClient();

            double totalProfit = 0.0;
            int count = 0;
            String line = "";

            try (S3Object object = amazonClient.getObject(bucketName, fileKey);
                 InputStreamReader streamReader = new InputStreamReader(object.getObjectContent(),
                         StandardCharsets.UTF_8);
                 BufferedReader reader = new BufferedReader(streamReader)) {

                while ((line = reader.readLine()) != null) {
                    if (count > 0) {
                        String[] cols = line.split(";");
                        totalProfit += Double.parseDouble(cols[6]);
                    }
                    count++;

                }

            } catch (final IOException e) {
                System.out.println("IOException: " + e.getMessage());
                context.getLogger().log("IOException: " + e.getMessage());
            }

            try (S3Object object = amazonClient.getObject(bucketName, fileKey);
                 InputStreamReader streamReader = new InputStreamReader(object.getObjectContent(),
                         StandardCharsets.UTF_8);
                 BufferedReader reader = new BufferedReader(streamReader)) {

                count = 0;
                line = "";

                count = 0;
                ArrayList<ProductData> products = new ArrayList<ProductData>();
                while ((line = reader.readLine()) != null) {
                    if (count > 0) {
                        String[] cols = line.split(";");
                        String nameToMatch = cols[2];
                        try {
                            int index = IntStream.range(0, products.size())
                                    .filter(i -> nameToMatch.equals(products.get(i).getName())).findFirst().getAsInt();
                            ProductData existingProduct = products.get(index);
                            ProductData newProduct = new ProductData(cols[2], Double.parseDouble(cols[3]),
                                    Double.parseDouble(cols[4]), Double.parseDouble(cols[6]));
                            existingProduct.incrementAll(newProduct);
                            products.set(index, existingProduct);
                        } catch (NoSuchElementException e) {
                            ProductData newProduct = new ProductData(cols[2], Double.parseDouble(cols[3]),
                                    Double.parseDouble(cols[4]), Double.parseDouble(cols[6]));
                            products.add(newProduct);
                        }
                    }
                    count++;
                }
                Parser.lambdaWorkerCSV(totalProfit, products, "summary-" + fileKey, client, context);

            } catch (final Exception e) {
                System.out.println("Exception: " + e.getMessage());
                context.getLogger().log("IOException: " + e.getMessage());
            }

            System.out.println("Finished... processing file");
            context.getLogger().log("Finished... processing file");
            DeleteObjectRequest deleteRequest = DeleteObjectRequest.builder()
                    .bucket(bucketName)
                    .key(fileKey)
                    .build();

            client.deleteObject(deleteRequest);
            System.out.println(fileKey + " deleted from bucket:" + bucketName);
            context.getLogger().log(fileKey + " deleted from bucket:" + bucketName);

        } else {
            System.out.println("No message found in the latest request");
            context.getLogger().log("No message found to process ");
        }

        timeStamp = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").format(Calendar.getInstance().getTime());
        context.getLogger().log("Invocation completed: " + timeStamp);

        return null;
    }
}
