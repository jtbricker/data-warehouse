import logging
import json
import configparser
import boto3
from botocore.exceptions import ClientError

def setup_logging():
    LOGGER.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
        handlers=[
            logging.FileHandler("./%(filename)s.log"),
            logging.StreamHandler()
        ])
    return logging.getLogger()

def setup_global_config(config_file):
    LOGGER.info('Getting config from %s', config_file)
    config = configparser.ConfigParser()
    config.read(config_file)
    return config

def create_ec2_client(config):
    return boto3.resource('ec2',
                          region_name="us-east-2",
                          aws_access_key_id=config['AWS']['KEY'],
                          aws_secret_access_key=config['AWS']['SECRET']
                          )

def create_redshift_client(config):
    return boto3.resource('redshift',
                          region_name="us-east-2",
                          aws_access_key_id=config['AWS']['KEY'],
                          aws_secret_access_key=config['AWS']['SECRET']
                          )
def create_redshift_instance(redshift, config, iam_role_arn):
    try:
        response = redshift.create_cluster(        
            #HW
            ClusterType=config['DWH']['DWH_CLUSTER_TYPE'],
            NodeType=config['DWH']['DWH_NODE_TYPE'],
            NumberOfNodes=int(config['DWH']['DWH_NUM_NODES']),

            #Identifiers & Credentials
            DBName=config['DWH']['DWH_DB'],
            ClusterIdentifier=config['DWH']['DWH_CLUSTER_IDENTIFIER'],
            MasterUsername=config['DWH']['DWH_DB_USER'],
            MasterUserPassword=config['DWH']['DWH_DB_PASSWORD'],
            
            #Roles (for s3 access)
            IamRoles=[iam_role_arn]  
        )
    except Exception as e:
        print(e)

def create_iam_client(config):
    return boto3.resource('IAM',
                          region_name="us-east-2",
                          aws_access_key_id=config['AWS']['KEY'],
                          aws_secret_access_key=config['AWS']['SECRET']
                          )
def create_iam_role(iam, config):
    role_name = config['IAM_ROLE']['ROLE_NAME']
    try:
        LOGGER.info("Creating a new IAM Role") 
        dwh_role = iam.create_role(
            Path='/',
            RoleName=role_name,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                                'Effect': 'Allow',
                                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        )    
    except Exception as e:
        print(e)

    LOGGER.info("Attaching Policy")
    iam.attach_role_policy(RoleName=role_name,
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                          )['ResponseMetadata']['HTTPStatusCode']

    LOGGER.info("Get the IAM role ARN")
    return iam.get_role(RoleName=role_name)['Role']['Arn']


def main():
    config = setup_global_config('dwh.cfg')

    iam = create_iam_client(config)
    roleArn = create_iam_role(iam, config)

    ec2 = create_ec2_client(config)
    redshift = create_redshift_client(config)

if __name__ == "__main__":
    LOGGER = setup_logging()

    LOGGER.info("Starting etl.py")
    main()