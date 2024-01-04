package emse.aws;

import software.amazon.awssdk.services.s3.S3Client;

import java.io.*;
import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.stream.IntStream;

import static emse.aws.Parser.*;

public class WorkerOrganizer {

    public static void main(String[] args) {
        S3Client client = S3Client.builder().region(ShopInventory.region).build();
        System.out.println("Processing: " + ShopInventory.salesData + "01-10-2022-store1.csv");

        double totalProfit = 0.0;
        int count = 0;
        String line = "";
        try (BufferedReader reader = new BufferedReader(
                new FileReader(ShopInventory.salesData + "01-10-2022-store1.csv"))) {

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

        try (BufferedReader reader = new BufferedReader(
                new FileReader(ShopInventory.salesData + "01-10-2022-store1.csv"))) {

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
            workerCSV(totalProfit, products, "mm-summary.csv", client);

        } catch (final IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }

        System.out.println("Finished... processing file");



    }

}
