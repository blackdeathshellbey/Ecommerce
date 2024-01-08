# Cloud Computing

Authors : Mohsen HADAVI, Erfan ENFERAD, Said GHATTAS
Demostration vidéo : http://
Project made with JAVA

	Java version : 17

To execute the project, EC2 Worker and EC2 Client with jar files the Client and the Worker will work as follows: 

	cd Client
	java -jar Client.jar
	
	cd Worker
	java -jar Worker.jar

The client uploads the file to S3 bucket and passed as parameter into the storage and notify the Worker.
The Worker applications one runnuning on EC2 and other on Lambda, reads the data and summarizes the sales.
Consolidator reads S3 buckets and provides a intell about profit.
