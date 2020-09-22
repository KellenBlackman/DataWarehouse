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
    """Creates an EC2 resource on AWS using the Boto3 package and AWS KEY and Secret information.

    Returns:
        boto3 resource: EC2 resource for given AWS instance.
    """
    try:
        ec2_resource = boto3.resource('ec2',
                                      region_name="us-west-2",
                                      aws_access_key_id=KEY,
                                      aws_secret_access_key=SECRET
                                      )
        return ec2_resource
    except Exception as e:
        print(e)


def create_iam_resource():
    """Creates an IAM resource on AWS using the Boto3 package and AWS Key and Secret information.

    Returns:
        boto3 resource: IAM resource for given AWS instance.
    """
    try:
        iam_resource = boto3.client('iam',
                                    aws_access_key_id=KEY,
                                    aws_secret_access_key=SECRET,
                                    region_name='us-west-2'
                                    )
        return iam_resource
    except Exception as e:
        print(e)


def create_redshift_resource():
    """Creates a Redshift resource on AWS using the Boto3 package and AWS Key and Secret information.

    Returns:
        boto3 resource: Redshift resource for given AWS instance.
    """
    try:
        redshift_resource = boto3.client('redshift',
                                         region_name="us-west-2",
                                         aws_access_key_id=KEY,
                                         aws_secret_access_key=SECRET
                                         )
        return redshift_resource
    except Exception as e:
        print(e)


def create_dwh_iam_role(iam_resource, role_name):
    """Creates an IAM Role with given role name.

    Parameters:
        iam_resource(boto3 resource): Iam resource for AWS instance.
        role_name(str): Name of the role to create.
    """
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
    """Attaches an AWS policy to the named IAM role.

    Parameters:
        policy_arn(str): The Amazon Resource Name for the policy to attach to the role.
        iam_resource(boto3 resource): The Iam Resource that contains the role to attach given policy.
        role_name(str): The name of the role to attach given policy.
    """
    try:
        iam_resource.attach_role_policy(
            RoleName=role_name,
            PolicyArn=policy_arn
        )
    except Exception as e:
        print(e)


def get_iam_role_arn(iam_resource, role_name):
    """Retrieves the Amazon Resource Name of named role contained withing given IAM Resource.

    Parameters:
        iam_resource(boto3 resource): The IAM Resource that contains the named role.
        role_name(str): The name of the role to retrieve ARN.

    Returns:
        str: The Amazon Resource Name of the given role.
    """
    role = iam_resource.get_role(RoleName=role_name)
    DWH_IAM_ROLE_ARN = role['Role']['Arn']
    return DWH_IAM_ROLE_ARN


def create_redshift_cluster(redshift_resource, iam_roles):
    """Creates Redshift Cluster with associated Roles and then prints to screen the Cluster Endpoint once available.

    Parameters:
        redshift_resource(boto3 resource): The Redshift Resource to house the database cluster.
        iam_roles(list of strings): The IAM Roles to attach to the Redshift cluster.
    """
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
        print("DWH_ENDPOINT :: ", DWH_ENDPOINT)


def get_redshift_cluster_props(redshift_resource, cluster_identifier):
    """Retrieves the details of a given Redshift Cluster.

    Parameters:
        redshift_resource(boto3 resource): The Redshift Resource that contains the DWH cluster.
        cluster_identifier(str): The DWH identifier on Redshift that we need information on.

    Returns:
        dict: Dictionary of cluster details given by boto3 describe clusters method.
    """
    try:
        cluster_info = redshift_resource.describe_clusters(
            ClusterIdentifier=cluster_identifier
        )['Clusters'][0]
    except Exception as e:
        print(e)
    return cluster_info


def open_tcp_port(ec2_resource, port, vpc_id):
    """Opens a TCP Port to access the cluster endpoint.

    Parameters:
        ec2_resource(boto3 resource): The EC2 Resource that will allow access to the DWH.
        port(str): The port number to give access to Cluster.
        vpc_id(str): The Virtual Private Cloud identifier that contains the DWH.
    """
    try:
        vpc = ec2_resource.Vpc(id=vpc_id)
        defaultSg = list(vpc.security_groups.all())[0]
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(port),
            ToPort=int(port)
        )
    except Exception as e:
        print(e)


def delete_cluster(redshift_resource, cluster_id):
    """Deletes identified cluster on the given Redshift resource.

    Parameters:
        redshift_resource(boto3 resource): The Redshift resource that holds the DWH cluster to be deleted.
        cluster_id(str): The Cluster Identifier that needs to be deleted.
    """
    try:
        redshift_resource.delete_cluster(
            ClusterIdentifier=cluster_id,  SkipFinalClusterSnapshot=True)
    except Exception as e:
        print(e)


def detach_policy_from_role(iam_resource, policy_arn, role_name):
    """Detaches Amazon Named Policy from given role.

    Parameters:
        iam_resource(boto3 resource): The IAM Resource that holds the given role.
        policy_arn(str): The policy Amazon Resource Name that needs to be detached from role.
        role_name(str): The named role that will have the given policy detached.
    """
    try:
        iam_resource.detach_role_policy(
            RoleName=role_name, PolicyArn=policy_arn)
    except Exception as e:
        print(e)


def teardown_data_warehouse(redshift_resource, iam_resource):
    """Removes all resources from DataWarehouse to limit costs.

    Parameters:
        redshift_resource(boto3 resource): The Redshift resource that contains the DataWarehouse.
        iam_resource(boto3 resource): The IAM Resource that contains the DWH Role.
    """
    delete_cluster(redshift_resource=redshift_resource,
                   cluster_id=DWH_CLUSTER_IDENTIFIER)
    detach_policy_from_role(iam_resource=iam_resource,
                            policy_arn=S3_READ_ONLY_POLICY_ARN, role_name=DWH_IAM_ROLE_NAME)
    iam_resource.delete_role(RoleName=DWH_IAM_ROLE_NAME)


def setup_data_warehouse(iam_resource, redshift_resource, ec2_resource):
    """Creates the data warehouse cluster and role given IAM, Redshift, and EC2 resources.

    Parameters:
        iam_resource(boto3 resource): The IAM Resource to hold the DWH Role.
        redshift_resource(boto3 resource): The Redshift Resource to house the DWH cluster. 
        ec2_resource(boto3 resource): The EC2 Resource that will direct traffic to the DWH cluser.
    """
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
                  vpc_id=vpc_id)


def main():
    # Create Iam Resource
    iam_resource = create_iam_resource()

    # Create Redshift Resource
    redshift_resource = create_redshift_resource()

    # Create EC2 Resource
    ec2_resource = create_ec2_resource()

    # Set up Data Warehouse
    setup_data_warehouse(iam_resource, redshift_resource, ec2_resource)

    # Tear down Data Warehouse
    # teardown_data_warehouse(redshift_resource=redshift_resource, iam_resource=iam_resource)


if __name__ == "__main__":
    main()
