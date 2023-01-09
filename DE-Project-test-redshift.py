# %%
#Imports
import boto3
import pandas as pd
import psycopg2
import json
import configparser

# %%
config = configparser.ConfigParser()

# %%
# Add the 'AWS' and 'DWH' section
config.add_section('AWS')
config.add_section('DWH')

# Set some options in the 'AWS' section
config.set('AWS', 'KEY', '<Your AWS ACCESS KEY>')
config.set('AWS', 'SECRET', '<Your AWS SECRET ACCESS KEY>')

# Set some options in the 'DWH' section
config.set('DWH', 'DWH_CLUSTER_TYPE', 'single-node')
config.set('DWH', 'DWH_NUM_NODES', '1')
config.set('DWH', 'DWH_NODE_TYPE', 'dc2.large')
config.set('DWH', 'DWH_CLUSTER_IDENTIFIER', 'my-first-redshift')
config.set('DWH', 'DWH_DB', 'myfirstdb')
config.set('DWH', 'DWH_DB_USER', 'aswuser')
config.set('DWH', 'DWH_DB_PASSWORD', 'Passw0rd123')
config.set('DWH', 'DWH_PORT', '5439')
config.set('DWH', 'DWH_IAM_ROLE_NAME', 'redshift-s3-access')

# Write the configuration file to disk
with open('cluster.config', 'w') as config_file:
    config.write(config_file)

# %%
#Get the values in 'cluster.config' and save then in variables
KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')

DWH_CLUSTER_TYPE = config.get('DWH', 'DWH_CLUSTER_TYPE')
DWH_NUM_NODES = config.get('DWH', 'DWH_NUM_NODES')
DWH_NODE_TYPE = config.get('DWH', 'DWH_NODE_TYPE')
DWH_CLUSTER_IDENTIFIER = config.get('DWH', 'DWH_CLUSTER_IDENTIFIER')
DWH_DB = config.get('DWH', 'DWH_DB')
DWH_DB_USER = config.get('DWH', 'DWH_DB_USER')
DWH_DB_PASSWORD = config.get('DWH', 'DWH_DB_PASSWORD')
DWH_PORT = config.get('DWH', 'DWH_PORT')
DWH_IAM_ROLE_NAME = config.get('DWH', 'DWH_IAM_ROLE_NAME')

(DWH_DB_USER, DWH_DB_PASSWORD, DWH_DB)


# %%
#Using pandas to better visualize the param and values
pd.DataFrame({'Param':
                    ['DWH_CLUSTER_TYPE', 'DWH_NUM_NODES', 'DWH_NODE_TYPE', 'DWH_CLUSTER_IDENTIFIER', 'DWH_DB', 'DWH_DB_USER', 'DWH_DB_PASSWORD', 'DWH_PORT', 'DWH_IAM_ROLE_NAME'],
              'Value':
                    [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME]  
            })

# %%
#Login in ec2, S3, IAM and Redshift
ec2 = boto3.resource('ec2',
                        region_name='sa-east-1',
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                    ) 

s3 = boto3.resource('s3',
                    region_name='sa-east-1',
                    aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET
                    )

iam = boto3.client('iam',
                    region_name='sa-east-1',
                    aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET
                    )

redshift = boto3.client('redshift',
                    region_name='sa-east-1',
                    aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET
                    )

# %%
#Analyze the files in the bucket
bucket = s3.Bucket('math-test-bucket')
log_data_files = [filename.key for filename in bucket.objects.filter(Prefix='')]
log_data_files

# %%
#Get the role in IAM
roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
roleArn

# %%
#Create a Redshift Cluster
try:
    response = redshift.create_cluster(
    #Cluster & Node Type
    ClusterType = DWH_CLUSTER_TYPE,
    NodeType = DWH_NODE_TYPE,

    #Identifiers & Credentials
    DBName = DWH_DB,
    ClusterIdentifier = DWH_CLUSTER_IDENTIFIER,
    MasterUsername = DWH_DB_USER,
    MasterUserPassword = DWH_DB_PASSWORD,

    #Roles (for s3 access)
    IamRoles = [roleArn]
    )
except Exception as e:
    print(e)

# %%
#Look the Cluster informations
redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

# %%
def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ['ClusterIdentifier', 'NodeType', 'ClusterStatus', 'MasterUsername',
                    'DBName', 'Endpoint', 'VpcId']
    x = [(k, v) for k, v in props.items() if k in keysToShow]
    return pd.DataFrame(data = x, columns = ['Key', 'Value'])

myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
prettyRedshiftProps(myClusterProps)

# %%
#Create variables
DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
DB_NAME = myClusterProps['DBName']
DB_USER = myClusterProps['MasterUsername']

# %%
try:
    vpc = ec2.Vpc(id=myClusterProps['VpcId'])
    defaultSg = list(vpc.security_groups.all())[0]
    print(defaultSg)

    defaultSg.authorize_ingress(GroupName = defaultSg.group_name,
                                CidrIp = '0.0.0.0/0',
                                IpProtocol = 'TCP',
                                FromPort = int(DWH_PORT),
                                ToPort = int(DWH_PORT)
                                )
except Exception as e:
    print(e)

# %%
#Try to make a connection to the Postgres
try:
    conn = psycopg2.connect(host = DWH_ENDPOINT, dbname = DB_NAME, 
                            user = DB_USER, password = DWH_DB_PASSWORD,
                            port = DWH_PORT
                            )
except psycopg2.Error as e:
    print('Error: Could not make connection to the Postgres Database')
    print(e)

conn.set_session(autocommit=True)

# %%
#Try to get curser to the Database
try:
    cur = conn.cursor()
except psycopg2.Error as e:
    print('Error: Could not get curser to the Database')
    print(e)

# %%
#Create users table 
try:
    cur.execute("""create table users(
        userid integer not null distkey sortkey,
        username char (8),
        firstname varchar(30),
        lastname varchar(30),
        city varchar(30),
        state char(2),
        email varchar(100),
        phone char(14),
        likesports boolean,
        liketheatre boolean,
        likeconcerts boolean,
        likejazz boolean,
        likeclassical boolean,
        likeopera boolean,
        likerock boolean,
        likevegas boolean,
        likebroadway boolean,
        likemusicals boolean);""")
except psycopg2.Error as e:
    print("Error: Issue creating table")
    print(e)

# %%
#Create venue table
try:
    cur.execute("""create table venue(
        venueid smallint not null distkey sortkey,
        venuename varchar(100),
        venuecity varchar(30),
        venuestate char(2),
        venueseats integer);""")
except psycopg2.Error as e:
    print("Error: Issue creating table")
    print(e)

# %%
#Create other tables 
try:
    cur.execute("""create table category(
        catid smallint not null distkey sortkey,
        catgroup varchar(10),
        catname varchar(10),
        catdesc varchar(50));

create table date(
        dateid smallint not null distkey sortkey,
        caldate date not null,
        day character(3) not null,
        week smallint not null,
        month character(5) not null,
        qtr character(5) not null,
        year smallint not null,
        holiday boolean default('N'));

create table event(
        eventid integer not null distkey,
        venueid smallint not null,
        catid smallint not null,
        dateid smallint not null sortkey,
        eventname varchar(200),
        starttime timestamp);

create table listing(
        listid integer not null distkey,
        sellerid integer not null,
        eventid integer not null,
        dateid smallint not null sortkey,
        numtickets smallint not null,
        ´riceperticket decimal(8,2),
        listtime timestamp);
""")

except psycopg2.Error as e:
    print("Error: Issue creating table")
    print(e)

# %%
#
try:
    cur.execute("""
    copy users from 's3://math-test-bucket/allusers_pipe.txt'
    credentials 'aws_iam_role=arn:aws:iam::248652697441:role/redshift-s3-access'
    delimiter '|'
    region 'sa-east-1'
    
    """)
except psycopg2.Error as e:
    print("Error: Issue creating table")
    print(e)

# %%
#
try:
    cur.execute("""
    select * from users;
    
    """)
except psycopg2.Error as e:
    print("Error: Issue creating table")
    print(e)

# %%
#
row = cur.fetchone()
while row:
    print(row)
    row = cur.fetchone()
    break

# %%
#Close conn
try:
    conn.close()
except psycopg2.Error as e:
    print(e)

# %%
#Deleting Cluster
redshift.delete_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot=True)


