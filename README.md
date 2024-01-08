# Cloud Computing

Authors : Mohsen HADAVI, Erfan ENFERAD, Said GHATTAS

Demostration video : http://

Project made with JAVA

	Java version: 17

To execute the project, EC2 Worker and EC2 Client with jar files the Client and the Worker will work as follows: 

	cd Client
	java -jar Client.jar
	
	cd Worker
	java -jar Worker.jar

The client uploads the file to the S3 bucket, passes it as a parameter into the storage, and notifies the Worker.

The Worker applications one running on EC2 and the other on Lambda, read the data and summarize the sales.

Consolidator reads S3 buckets and provides intel about profit.
