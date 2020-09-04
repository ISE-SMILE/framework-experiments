# Setup for Corral

 1. enter a query folder
 2. setup AWS credentials as discribed in (Corral Readme)[https://github.com/bcongdon/corral#aws-credentials]
 3. run `go build -o $(PROG_NAME) . && ./$(PROG_NAME) --lambda --out "s3://<output bucket>" "s3://<inputbucket>/*"` to run the query

## Optional
Corral can be configured through the `corralrc.yml` file, see (this)[https://github.com/bcongdon/corral#configuration] for more options.
