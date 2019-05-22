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
            logging.FileHandler("./dwh.log"),
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
    return boto3.client('redshift',
                          region_name="us-east-2",
                          aws_access_key_id=config['AWS']['KEY'],
                          aws_secret_access_key=config['AWS']['SECRET']
                          )

def create_redshift_instance(redshift, config, iam_role_arn):
    LOGGER.info("Creating redshift instance")
    try:
        redshift.create_cluster(        
            ClusterType=config['CLUSTER']['CLUSTER_TYPE'],
            NodeType=config['CLUSTER']['NODE_TYPE'],
            NumberOfNodes=int(config['CLUSTER']['NUM_NODES']),
            DBName=config['CLUSTER']['DB_NAME'],
            ClusterIdentifier=config['CLUSTER']['CLUSTER_IDENTIFIER'],
            MasterUsername=config['CLUSTER']['DB_USER'],
            MasterUserPassword=config['CLUSTER']['DB_PASSWORD'],
            IamRoles=[iam_role_arn]  
        )
       
    except Exception as e:
        LOGGER.error(e)

def create_iam_client(config):
    LOGGER.info("Creating iam client")    
    return boto3.client('iam',
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
        LOGGER.error(e)

    LOGGER.info("Attaching Policy")
    iam.attach_role_policy(RoleName=role_name,
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                          )['ResponseMetadata']['HTTPStatusCode']

    LOGGER.info("Get the IAM role ARN")
    return iam.get_role(RoleName=role_name)['Role']['Arn']

def open_port(ec2, config, vpc_id):
    try:
        port = config['CLUSTER']['DB_PORT']
        LOGGER.info("Opening port %s", port)        
        vpc = ec2.Vpc(id=vpc_id)
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

def pretty_redshift_props(props):
    keys_to_show = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    
    x = ["%s: %s" %(k, v) for k,v in props.items() if k in keys_to_show]
    return x

def main():
    config = setup_config('dwh.cfg')

    iam = create_iam_client(config)
    role_arn = create_iam_role(iam, config)

    redshift = create_redshift_client(config)
    create_redshift_instance(redshift, config, role_arn)

    ec2 = create_ec2_client(config)

    my_cluster_props = redshift.describe_clusters(ClusterIdentifier=config['CLUSTER']['CLUSTER_IDENTIFIER'])['Clusters'][0]
    vpc_id = my_cluster_props['VpcId']
    open_port(ec2, config, vpc_id)

    LOGGER.info(pretty_redshift_props(my_cluster_props))

if __name__ == "__main__":
    LOGGER = setup_logging()

    LOGGER.info("Starting etl.py")
    main()
    