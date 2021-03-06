## Lab3 : Evaluation for the Cloud Computing module
**Azzeddine Abdallah ZEDDOURI   
Ecole des Mines de Saint-Etienne (CPS2).  
contact: a.a.zeddouri@gmail.com** 


Requirements:  
-python3.  
-boto3.  
-ImageMagick 6.9.10-23  
-Aws cli  
  
Debug environement :  
 -Ubuntu 20.04.1 LTS / Amazon Linux 

# Files Description :
* connect.sh to connect to the EC2 instance via ssh
* cle1.pem: ssh key to connect the EC2 instance
* processing_image_server.py : the program hadling the requests, performing the image processing
* myclient.py : the client performing the requests, it has  a simple CLI.
* ./input directory
* ./output directory
* image.jpeg : a sample input image
* 1.png a samle output image

# Some Precisions :
Operations performed by the EC2 instance are logged in json like format in the file 
"ConvertImagesLog.txt", everytime this file exceeds 5MB size it's saverd in a S3 Bucket named "mylogbucket0"
and a new log file is created.

After processing and being downloaded by the client, files are immediatly deleted from the EC2 instance and from S3 Buckets, 
same thing for corresponding Queue messages, (altought temporary advanced storage management might be done),
only the logs are stored permanently.

the image processing is performed by ImageMagick 6.9.10-23, and consists of type 
convertion and text tagging on the input image.

The aws_access_key belongs to an IAM User with restricted privileges.
"user_accessKeys.csv" file sent to you by email allows access to the needed aws services.
on the EC2 instance it's preconfigured you need only to run

The Security Group rules of the EC2 instance is are relatively weak for testing reasons, all Inbound TCP/22 communication are allowed,
and all TCP/Any communication are allowed.

**To test :**  
  1-connect to the EC2 instance via SSH using "connect.sh" and "cle1.pem" (sent to you by email);  
  2-Run the server ("python3 processing_image_server.py");
  4-configure the given credentials on the "myclient.py" machine (or hardcod it).
  3-Run the client and follow instructions ("python3 myclient.py").  
  
**Please note** : the EC2 instance is kept up for testing puposes;  
                   several TimeOuts have been inserted on both server and client programs on Purpose to deal with the delay issues, please Be Patient.
              

