## Golang Parquet Example

Sample repository showing a simple use case for parsing an Apache Parquet format file using the Go library [parquet-go](https://github.com/fraugster/parquet-go).  A supporting [Blog Article](https://www.binaryheap.com/nvqj) goes into further depth about the contents of the repos.

  * Environment variables exist in the `launch.json` file for executing
    * SAMPLE_BUCKET - the s3 bucket where your parquet file is
    * SAMPLE_KEY - the s3 key for the file. This 
    * AWS_PROFILE - the profile to be used when connecting to AWS
  * Project could be converted to run multiple files, scan a bucket or even converted into a Lambda