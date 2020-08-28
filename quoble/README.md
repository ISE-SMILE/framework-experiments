# Setup for Quoble
### general setup
+ Goto iam, Create role, Choose Lambda as the service, give Permissions to: AmazonEC2FullAccess,AWSLambdaFullAccess,AmazonS3FullAccess,AWSLambdaExecute,AWSLambdaVPCAccessExecutionRole,AWSLambdaRole
+ Add tags for cost review and name the role e.g “spark-lambda”
+ Create a VPC
+ create a NAT in the VPC which forwards all traffic
+ create a routing table which routes all traffic to the NAT
+ create a routing table which routes all traffic to the igw
+ there should be 3 subnets in the VPC  by default if not create them
+ make one subnet private by setting the routing table to the one that forwards everything to the NAT 
+ for the other subnets set the routing table which forwards to the igw
+ create an endpoint that forwards all traffic in the VPC
+ generate AWS credentials
+ on your local machine clone the [quoble github repo](https://github.com/qubole/spark-on-lambda/) 
+ download the prepackaged tar from the quoble bucket (s3://public-qubole/lambda/spark-2.1.0-bin-spark-lambda-2.1.0.tgz) 
+ run this command in the project folder to generate the runtime for the lambdas 
`bash -x bin/lambda/spark-lambda 149 spark-2.1.0-bin-spark-lambda-2.1.0.tgz s3://<your-s3-bucket>`
the former command automatically uploads the zip to your bucket just make sure aws is configured right on your local machine


### EC2 setup 
+ create an instance of at least size t3.medium as image use amazon linux
+ as security group choose default
+ make sure all traffic is forwarded
+ create a ssh cert to login to the instance later
+ put the instance into the VPC subnet with a routing table facing the igw(aka a public one)
+ connect to the instance via ssh using the generated cert 
`ssh -i “<path-to-cert>”  ec2-user@<your-ec2-address>`
+ the address of the EC2 instance can be found in the Instance dashboard and looks like this “ec2-18-157-183-130.eu-central-1.compute.amazonaws.com”
+ set your aws credentials using “aws configure”  and enter your previously generated credentials there, also set the region to eu-central-1
+ download the spark driver using 
``aws s3 cp s3://public-qubole/lambda/spark-2.1.0-bin-spark-lambda-2.1.0.tgz .``
unpack the tar
move into the unpacked folder
create or edit ~/spark-2.1.0-bin-spark-lambda/bin/conf/spark-defaults.conf to have the following content:
```
spark.dynamicAllocation.enabled                 true
spark.dynamicAllocation.minExecutors            2
spark.dynamicAllocation.maxExecutor             16
spark.shuffle.s3.enabled                        true
spark.lambda.concurrent.requests.max            100
spark.hadoop.fs.s3n.impl                        org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3.impl                         org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.AbstractFileSystem.s3.impl      org.apache.hadoop.fs.s3a.S3A
spark.hadoop.fs.AbstractFileSystem.s3n.impl     org.apache.hadoop.fs.s3a.S3A
spark.hadoop.fs.AbstractFileSystem.s3a.impl     org.apache.hadoop.fs.s3a.S3A
spark.hadoop.qubole.aws.use.v4.signature        true
spark.hadoop.fs.s3a.fast.upload                 true
spark.lambda.function.name                      spark-lambda
spark.lambda.spark.software.version             149
spark.hadoop.fs.s3a.endpoint                    s3.eu-central-1.amazonaws.com
spark.hadoop.fs.s3n.awsAccessKeyId              <YOUR ACCESS KEY>
spark.hadoop.fs.s3n.awsSecretAccessKey          <YOUR SECRET>
spark.shuffle.s3.bucket                         s3://<your-bucket>
spark.lambda.s3.bucket                          s3://<your-bucket>
```
### Lambda Setup
+ create a lambda function from scratch as runtime use python 2.7
+ the security group of the lambda needs to match the one of the ec2
+ choose your created VPC as VPC as subnet choose the one with the routing table pointing to the NAT(aka the private one)
+ set the memory of the function to 1024 MB and the timeout to 5 min
+ copy [this](https://github.com/faromero/spark-on-lambda/blob/lambda-2.1.0/bin/lambda/spark-lambda-os.py) code into the function code field  
+ set the handler to match the new filename
+ add HOSTALIASES with the value  /tmp/HOSTALIASES to the environment

