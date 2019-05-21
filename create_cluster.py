import logging
import json
import configparser
import boto3
from botocore.exceptions import ClientError

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
        handlers=[
            logging.FileHandler("./%(filename)s.log"),
            logging.StreamHandler()
        ])
    return logging.getLogger()

def setup_config(config_file):
    LOGGER.info('Getting config from %s', config_file)
    config = configparser.ConfigParser()
    config.read(config_file)
    return config

def create_ec2_client(config):
    LOGGER.info("Creating ec2 client")    
    return boto3.resource('ec2',
                          region_name="us-east-2",
                          aws_access_key_id=config['AWS']['KEY'],
                          aws_secret_access_key=config['AWS']['SECRET']
                          )

def create_redshift_client(config):
    LOGGER.info("Creating redshift client")    
    return boto3.resource('redshift',
                          region_name="us-east-2",
                          aws_access_key_id=config['AWS']['KEY'],
                          aws_secret_access_key=config['AWS']['SECRET']
                          )

def create_redshift_instance(redshift, config, iam_role_arn):
    LOGGER.info("Creating redshift instance")
    try:
        redshift.create_cluster(        
            ClusterType=config['DWH']['DWH_CLUSTER_TYPE'],
            NodeType=config['DWH']['DWH_NODE_TYPE'],
            NumberOfNodes=int(config['DWH']['DWH_NUM_NODES']),
            DBName=config['DWH']['DWH_DB'],
            ClusterIdentifier=config['DWH']['DWH_CLUSTER_IDENTIFIER'],
            MasterUsername=config['DWH']['DWH_DB_USER'],
            MasterUserPassword=config['DWH']['DWH_DB_PASSWORD'],
            IamRoles=[iam_role_arn]  
        )
    except Exception as e:
        LOGGER.error(e)

def create_iam_client(config):
    LOGGER.info("Creating iam client")    
    return boto3.resource('iam',
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

def open_port(redshift, ec2, config):
    cluster = config['DWH']['DWH_CLUSTER_IDENTIFIER']
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=cluster)['Clusters'][0]

    try:
        port = config['DWH']['DWH_PORT']
        LOGGER.info("Opening port %s", port)        
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        LOGGER.debug(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(port),
            ToPort=int(port)
        )

    except Exception as e:
        LOGGER.error(e)

def main():
    config = setup_config('dwh.cfg')

    iam = create_iam_client(config)
    role_arn = create_iam_role(iam, config)

    redshift = create_redshift_client(config)
    create_redshift_instance(redshift, config, role_arn)

    ec2 = create_ec2_client(config)

    open_port(redshift, ec2, config)

if __name__ == "__main__":
    LOGGER = setup_logging()

    LOGGER.info("Starting etl.py")
    main()
    