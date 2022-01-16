
![logo_ironhack_blue 7](https://marvel-b1-cdn.bc0a.com/f00000000243109/1x5o5mujiug388ttap1p8s17-wpengine.netdna-ssl.com/wp-content/uploads/2020/12/AWS-logo-2-400x300.jpg)
# PROJECT | REAL TIME DATA STREAMING PIPELINE
## Write sample data to an AWS Kinesis data stream, trigger a Lambda function to process the data and insert it into an RDS MySQL Instance. Connect a Kinesis firehose to the data stream and store the raw data in an S3 bucket. Build a real time application to analyze the collected data in real-time
### Intoduction
Real time data streaming has become critical to many organization especialy those in retail. Organizations need to analyze data in real time and obtain actionable insights to respond to customer needs appropriately or obtain a competitive edge
Data streaming has become ubiquitous and critical to business success

## AWS Services used
1. Kinesis data stream
2. Lambda
3. AWS RDS MySQL database
4. S3
5. Kinesis real time application
6. Kinesis firehose

## Improvements
1. Create a glue catalogue table to be followed by the Kinesis firehose when insertind data into the S3 bucket
2. Append prefixes onto the data and use month names as partitions in S3 bucket where the firehose stores data
3. Connect the stream to AWS QuickSight for better visualization

