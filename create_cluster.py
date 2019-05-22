import logging
import json
import configparser
import boto3
from botocore.exceptions import ClientError

""" Configures and returns a logger object

Returns:
    [logger] -- configured logger object
"""
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
    """ Read configuration file
    
    Arguments:
        config_file {string} -- path to configuration file
    
    Returns:
        dictionary -- a dictionary of dictionary of categorized config values
    """
    LOGGER.info('Getting config from %s', config_file)
    config = configparser.ConfigParser()
    config.read(config_file)
    return config

def create_ec2_client(config):
    """Create a client to interact with AWS EC2
    
    Arguments:
        config {dictionary} -- configuration object
    
    Returns:
        client -- an aws ec2 client
    """
    LOGGER.info("Creating ec2 client")    
    return boto3.resource('ec2',
                          region_name="us-east-2",
                          aws_access_key_id=config['AWS']['KEY'],
                          aws_secret_access_key=config['AWS']['SECRET']
                          )

def create_redshift_client(config):
    """Create a client to interact with AWS Redshift
    
    Arguments:
        config {dictionary} -- configuration object
    
    Returns:
        client -- an aws redshift client
    """
    LOGGER.info("Creating redshift client")    
    return boto3.client('redshift',
                          region_name="us-east-2",
                          aws_access_key_id=config['AWS']['KEY'],
                          aws_secret_access_key=config['AWS']['SECRET']
                          )

def create_redshift_instance(redshift, config, iam_role_arn):
    """ Create a redshift cluster
    
    Arguments:
        redshift {redshift client} -- boto3 redshift client object
        config {dictionary} -- configuration object
        iam_role_arn {string} -- amazon resource name for a configured IAM role
    """
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
    """Create a client to interact with AWS IAM
    
    Arguments:
        config {dictionary} -- configuration object
    
    Returns:
        client -- an aws iam client
    """
    LOGGER.info("Creating iam client")    
    return boto3.client('iam',
                          region_name="us-east-2",
                          aws_access_key_id=config['AWS']['KEY'],
                          aws_secret_access_key=config['AWS']['SECRET']
                          )

def create_iam_role(iam, config):
    """ Creates an AWS IAM role
    
    Arguments:
        iam {boto3 iam client} -- iam client object
        config {dictionary} -- configuration object
    
    Returns:
        string -- iam role arn
    """
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
    """ Expose port for default security group
    
    Arguments:
        ec2 {ec2 client} -- boto3 ec2 client object
        config {dictionary} -- configuration object
        vpc_id {string} -- id of your cluster's virtual private cloud
    """
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
    """ Returns an easy-to-read representation of the redshift cluster properties
    
    Arguments:
        props {dictionary} -- dictionary of redshift cluster properties
    
    Returns:
        array -- An array of key-value pairs of redshift properties
    """
    keys_to_show = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    
    x = ["%s: %s" %(k, v) for k,v in props.items() if k in keys_to_show]
    return x

def main():
    """Setup configuration, create clients, create redshift cluster, and configure cluster for access
    """
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
    """Setup logging
    """
    LOGGER = setup_logging()

    LOGGER.info("Starting etl.py")
    main()
    