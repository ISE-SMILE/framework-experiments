# Baseline

The baselien is based on AWS EMR. First create a deployment packe using `mvn install && mvn package`. Then upload the generated jar file to S3.

Setup an EMR cluster using Apache Spark using 2 Worker and 1 Master, m5xlarge. Add the Jar as an Apache Spark Setp and use `--class Q1` or `--class Q6` to perfrom one of the TPC-H queries.