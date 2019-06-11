# DQ Count Extractor

A tool to produce the a table of data consisting of Received date, Zip count, XML count and	Uncompressed byte total of XMLs that exist in DQ between a given set of dates

## Inputs

The following inputs can be set in the job.yaml to be accessed in the python script as environment variables. 

* START_DATE - Start date in yyyy/mm/dd or yyyy-mm-dd format of the ingest that we want the script to target

* END_DATE - End date in yyyy/mm/dd or yyyy-mm-dd format of the ingest that we want the script to target

* POOL_SIZE - Holds the pool size setting for the script, by default set to 8 in the job.yaml file. This can be increased for quicker processing if targeting a wider date range

## Secrets

Secrets are captured in the secrets.yaml file and can either be specified at run time when deploying the file in a kubernetes cluster or directly in the same file. They have to be encoded to base64.

## Deployment

The script is built as a docker image, pushed to quay.io/ukhomeofficedigital repository and run as a Job in ACP. Following are the steps to deploy the job.

Step 1: git clone the repo

Step 2: docker build -t dqe .

Step 3: docker tag dqe quay.io/ukhomeofficedigital/dq-count-extractor:latest

Step 4: docker push quay.io/ukhomeofficedigital/dq-count-extractor:latest

Step 5: Overwrite the secrets as applicable in the secrets.yaml

Step 6: kubectl --context=<context> --namespace=<namespace> create -f secrets.yaml

Step 7: Over write the start date, end date, pool size as applicable in the job.yaml

Step 8: kubectl --context=<context> --namespace=<namespace> create -f job.yaml

Step 9: Verify the job is triggered and pod created using the following and confirm that its status 'Completed'

kubectl --context=<context> --namespace=<namespace> get pods

Step 10: Check and record the stat output using the following

kubectl --context=<context> --namespace=<namespace> log <pod name>
