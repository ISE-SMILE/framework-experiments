# Kubernetes
First edit the Q1 and Q6 classes to match your minio deployment. Fill in the endpoint and your credentials.
After create a deployment package using `mvn install && mvn package`. Then build the docker images for the driver and the executor using `docker bulid -t <your tag> -f Dockerfile.driver . ` 
and `docker bulid -t <your tag> -f Dockerfile.exec . `. Upload the images to dockerhub or your own repo.

Make sure kubectl is correctly configured and a service account for your deployment exists. If kubectl is setup right you should be able to start a spark pod by using `kubectl run spark-test-pod -n spark -it --rm=true \
--image=<your driver image> \
--serviceaccount=<your service account> \
--command -- /bin/bash `

In the spawned bash execute `  /opt/spark/bin/spark-submit --name spark-tpc-q6 \
--master k8s://https://kubernetes.default:443 \
--deploy-mode cluster  \
--class Q1 \
--conf spark.kubernetes.driver.pod.name=spark-tpc-q1  \
--conf spark.kubernetes.authenticate.subdmission.caCertFile=/var/run/secrets/kubernetes.io/serviceaccount/ca.crt  \
--conf spark.kubernetes.authenticate.submission.oauthTokenFile=/var/run/secrets/kubernetes.io/serviceaccount/token  \
--conf spark.kubernetes.authenticate.driver.serviceAccountName=<your service account>  \
--conf spark.kubernetes.namespace=<your namespace>  \
--conf spark.executor.instances=10  \
--conf spark.kubernetes.container.image=<your executor image> \
--conf spark.kubernetes.container.image.pullPolicy=Always \
--conf spark.kubernetes.driverEnv.SPARK_EXECUTOR_MEMORY=1g \
local:///opt/spark/jars/Spark-TPC-1.0-SNAPSHOT.jar`
This command will spawn 10 executors with 1 GB memory each.
