import boto3
import configparser
import json
import time

config = configparser.ConfigParser()
config.read('aws_cred.cfg')

# AWS Creds
KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')

# Redshift Cluster Details
DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")
DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
DWH_DB = config.get("DWH", "DWH_DB")
DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")
DWH_PORT = config.get("DWH", "DWH_PORT")
DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")

# S3 Read Only Policy ARN
S3_READ_ONLY_POLICY_ARN = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"


def create_ec2_resource():
    ec2 = boto3.resource('ec2',
                         region_name="us-west-2",
                         aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET
                         )
    return ec2


def create_iam_resource():
    iam = boto3.client('iam',
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET,
                       region_name='us-west-2'
                       )
    return iam


def create_redshift_resource():
    redshift = boto3.client('redshift',
                            region_name="us-west-2",
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET
                            )
    return redshift


def create_dwh_iam_role(iam_resource, role_name):
    assume_role_policy_doc = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {
                "Service": ["ec2.amazonaws.com"]},
            "Action":["sts:AssumeRole"]}
        ]}
    try:
        iam_resource.create_role(
            Path='/',
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(assume_role_policy_doc),
            Description="Allows Redshift to call AWS Services"
        )
    except Exception as e:
        print(e)


def attach_policy_to_iam_role(policy_arn, iam_resource, role_name):
    iam_resource.attach_role_policy(
        RoleName=role_name,
        PolicyArn=policy_arn
    )


def get_iam_role_arn(iam_resource, role_name):
    r = iam_resource.get_role(RoleName=role_name)
    DWH_IAM_ROLE_ARN = r['Role']['Arn']
    return DWH_IAM_ROLE_ARN


def create_redshift_cluster(redshift_resource, iam_roles):
    myClusterProps = None
    try:
        redshift_resource.create_cluster(
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),
            DBName=DWH_DB,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            IamRoles=iam_roles
        )
        myClusterProps = redshift_resource.describe_clusters(
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    except Exception as e:
        print(e)

    if myClusterProps is not None:
        while(myClusterProps['ClusterStatus'] != 'available'):
            time.sleep(10)
            myClusterProps = get_redshift_cluster_props(
                redshift_resource=redshift_resource, cluster_identifier=DWH_CLUSTER_IDENTIFIER)

        DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
        DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
        print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
        print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)


def get_redshift_cluster_props(redshift_resource, cluster_identifier):
    return redshift_resource.describe_clusters(
        ClusterIdentifier=cluster_identifier
    )['Clusters'][0]


def open_tcp_port(ec2_resource, port, vpc_id):
    try:
        vpc = ec2_resource.Vpc(id=vpc_id)
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)

        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(port),
            ToPort=int(port)
        )
    except Exception as e:
        print(e)


def teardown_data_warehouse(redshift_resource, iam_resource):
    redshift_resource.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
    iam_resource.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn=S3_READ_ONLY_POLICY_ARN)
    iam_resource.delete_role(RoleName=DWH_IAM_ROLE_NAME)


def setup_data_warehouse(iam_resource, redshift_resource, ec2_resource):
    # Create Role to access S3 Bucket
    print("Creating New Iam Role")
    create_dwh_iam_role(iam_resource=iam_resource, role_name=DWH_IAM_ROLE_NAME)

    # Attach Read S3 Read Only Policy to Created Role
    print("Attaching S3 Read Policy to DWH_ROLE")
    attach_policy_to_iam_role(
        policy_arn=S3_READ_ONLY_POLICY_ARN,
        iam_resource=iam_resource,
        role_name=DWH_IAM_ROLE_NAME)

    # Get IAM Role ARN to Add to Redshift Cluster
    DWH_IAM_ROLE_ARN = get_iam_role_arn(
        iam_resource=iam_resource, role_name=DWH_IAM_ROLE_NAME)
    print("DWH_IAM_ROLE_ARN :: ", DWH_IAM_ROLE_ARN)

    # Create Redshift Cluster for DWH
    print("Creating Redshift Cluster")
    create_redshift_cluster(
        redshift_resource=redshift_resource, iam_roles=[DWH_IAM_ROLE_ARN])

    # Get Redshift VPC ID to add TCP Port
    redshift_cluster_props = get_redshift_cluster_props(
        redshift_resource=redshift_resource, cluster_identifier=DWH_CLUSTER_IDENTIFIER)
    vpc_id = redshift_cluster_props['VpcId']

    # Open Incoming TCP port to access Cluster Endpoint
    print("Opening TCP Port")
    open_tcp_port(ec2_resource=ec2_resource, port=DWH_PORT,
                  vpc_id = vpc_id)


def main():
    # Create Iam Resource
    iam_resource = create_iam_resource()

    # Create Redshift Resource
    redshift_resource = create_redshift_resource()

    # Create EC2 Resource
    ec2_resource = create_ec2_resource()

    # Set up Data Warehouse
    setup_data_warehouse(iam_resource, redshift_resource, ec2_resource)


if __name__ == "__main__":
    main()
