# DQ Count Extractor

A tool to produce the a table of data consisting of Received date, Zip count, XML count and	Uncompressed byte total of XMLs that exist in DQ between a given set of dates

## Inputs

The input date format for the start and end dates is dd/mm/yyyy and is specified in the job.yaml file

## Secrets

Secrets are captured in the secrets.yaml file and can either be specified at run time when deploying the file in a kubernetes cluster or directly in the same file. They have to be encoded to base64.

