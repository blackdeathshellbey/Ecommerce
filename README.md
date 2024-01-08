# Cloud Computing

**Authors:** Mohsen HADAVI, Erfan ENFERAD, Said GHATTAS

**Demonstration Video:** [Demo Video Link](https://drive.google.com/file/d/1TBT8IIwJdA1HSkELzoQDmu3gr_CkhsgX/view?usp=sharing)

**Project Language:** JAVA

```bash
Java Version: 17
```


**Note:**

- Every login (Start lab) triggers a change in server credentials. After starting the lab on AWS, it is necessary to navigate with `nano ./.aws/credentials` to obtain the cridentials.

**To execute the project:**

1. The EC2 Worker JAR file is stored in the cloud. An SSH connection is required for access.

```bash
java -jar worker.jar
```

2. The Client and Consolidator are operated and executed both locally.

3. The Client uploads the file to the S3 bucket, passes it as a parameter into the storage, and notifies the Worker.

4. The Worker applications (Lambda, EC2) read the data and summarize the sales.

5. Consolidator reads S3 buckets and provides intel about profit.

**Description of the project:**

**Client:**

- Used independently by each store.
- Uploads the daily file passed as parameter into the Cloud storage.
- Notifies the Worker application.

**Worker:**

- Reads the CSV file and summarizes the daily sales by store and by product.

- By Store:
    - Total profit

- By Product:
    - Total quantity
    - Total sold
    - Total profit

- Stores the summarized result in a new CSV file in the Cloud storage.

**Consolidator:**

- Executed manually once the operator detects that the summary files of all stores have been processed.
- Operator passes the date of the files to be processed as argument to the application.
- Reads the summary results from the files of that date.
- Displays the total retailer's profit, the most and least profitable stores, and the total quantity, total sold, and total profit per product.