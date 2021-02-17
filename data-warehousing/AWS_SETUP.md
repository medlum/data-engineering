
## Setup AWS

- Refer to `aws_setup.ipynb` for codes
- The setup will require `pandas`, `boto3`, `json` libraries

### Create IAM user
- Create a new IAM user in your AWS account
- Give it AdministratorAccess, From Attach existing policies directly Tab
- Take note of the access key and secret and edit it in the `aws_config.cfg` file  

### Load parameters from file
- Get the parameters from `aws_config.cfg` required for AWS setup
- `aws_config.cfg` should contain: 
    - Access to AWS clients: `access key`, `secret key` 
    - Setup for RedShift cluster: `cluster type`, `number of nodes`, `node type`
    - Access for IAM role: `role name`, `cluster identitifer`, `user name`, `password`, `port number`


### Create a IAM role 
- This enable Reshift to access S3 Bucket
- Set access to READ ONLY to resrict access rights

### Create clients for IAM, EC2, S3 and Reshift
- Use `boto3` library to connect AWS API
- Region name, access key and secrety key is required for all clients.

### Create Redshift cluster
- This step require a boiler code from `aws_setup.ipynb`
- Check the cluster status withthe boilder code

### Take note of the cluster endpoint and role ARN
- Use the boiler code to retrieve cluster endpoint and ARN
- ARN refers to Amazon Resource Names
- Example of endpoint `dwhcluster.cy2wnxrljw7h.us-west-2.redshift.amazonaws.com`
- Example of ARN `arn:aws:iam::988680640315:role/redShiftRole`
- For subsequent connections to Redshift, endpoint and ARN will be required for a new cfg file.
- Put endpoint in the `HOST=` file in new cfg file

