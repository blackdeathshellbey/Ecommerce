package emse.aws;

import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.TimerTask;
import java.util.stream.IntStream;

import static emse.aws.Parser.workerCSV;

public class WorkerProcessor extends TimerTask {

    private static S3Client s3Client;
    private static SqsClient sqsClient;

    @Override
    public void run() {
        s3Client = S3Client.builder().region(ShopInventory.region).build();
        sqsClient = SqsClient.builder().region(ShopInventory.region).build();

        // retrieve message of the inbox in order to see if a client sent data
        List<Message> messages = retrieveMessages(ShopInventory.sqsURL, sqsClient);
        if (messages.isEmpty()) {
            System.out.println("No Message Inbox !");
        } else {
            System.out.println("Found Message !");
            for (Message message : messages) {
                // declaration of variables with message content
                String messBody = message.body();
                String messWords[] = messBody.split(";", 2);

                // fetch data File in order to parse it
                GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(messWords[0])
                        .key(messWords[1]).build();
                String tempFileName = "csvFile" + "01-10-2022" + ".csv";
                String resultFileName = "resultFile" + "01-10-2022" + ".csv";
                File csvFile = new File(tempFileName);
                s3Client.getObject(getObjectRequest, ResponseTransformer.toFile(csvFile)); // the data is now in csvFile

                // generation of results with help of "ParseCSV library"
                processCSV(tempFileName, resultFileName);

                try{
                    // Push result in bucket
                    File resultFile = new File(resultFileName);
                    Path filePath = resultFile.toPath();
                    boolean fileUploaded = Bucket.uploadBucket(s3Client, ShopInventory.bucket, resultFileName, filePath);

                    if (fileUploaded){
                        resultFile.delete();
                        csvFile.delete();

                        // delete message in SQS
                        deleteMessage(message, ShopInventory.sqsURL);

                        // send message in order to notice the client there is result
                        sqsClient.sendMessage(SendMessageRequest.builder().queueUrl(ShopInventory.sqsOutboxURL)
                                .messageBody(ShopInventory.bucket + ";" + resultFileName).delaySeconds(10).build());
                        System.out.println("Message has been handled !");
                    }else{
                        System.out.println("Could not upload the result file to the "+ ShopInventory.bucket);
                    }

                }catch (Exception e){
                    System.out.println(e.getMessage());
                }
            }
        }
    }

    public static List<Message> retrieveMessages(String queueUrl, SqsClient sqsClient) {
        try {
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder().queueUrl(queueUrl)
                    .maxNumberOfMessages(5).build();
            List<Message> messages = sqsClient.receiveMessage(receiveMessageRequest).messages();
            return messages;
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return null;
    }

    public static void processCSV(String inputPath, String outputPath) {
        double totalProfit = 0.0;
        int count = 0;
        String line = "";

        // By store
        try (BufferedReader reader = new BufferedReader(
                new FileReader(inputPath))) {

            while ((line = reader.readLine()) != null) {
                if (count > 0) {
                    String[] cols = line.split(";");
                    totalProfit += Double.parseDouble(cols[6]);
                }
                count++;

            }
        } catch (final IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }

        // By Product
        try (BufferedReader reader = new BufferedReader(
                new FileReader(inputPath))) {

            count = 0;
            line = "";
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
            workerCSV(totalProfit, products, outputPath, s3Client);
            System.out.println("Processing the file has been finished !");

        } catch (final IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }


    public static void deleteMessage(Message message, String queueUrl) {
        try {
            DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder().queueUrl(queueUrl)
                    .receiptHandle(message.receiptHandle()).build();
            sqsClient.deleteMessage(deleteMessageRequest);

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }
}
