from cmd_helpers import *
import time
import argparse
import sys
import os

# TODO make public
AMI_IMAGE_ID = 'ami-9c956cfc'
SEC_GROUP_NAME = 'repro_rdd_secgroup'
INSTANCE_TYPE = 't2.micro' # TODO 'm4.xlarge'
IS_AWS_RUNNING = False
MASTER = None
SLAVES = []

def main():
  parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
      description="""Use EC2 to reproduce results from the Resilient Distributed Datasets (Spark) paper. Note: the created
instances are not designed to be secure, and it is not recommended to use them for anything else.""",
      epilog="""You can obtain your keys from https://console.aws.amazon.com/iam/home?region=us-west-2#security_credential
and http://docs.aws.amazon.com/general/latest/gr/managing-aws-access-keys.html contains more information.
AWS recommends creating IAM users, but if you trust this script you can just use your root account keys.""")
  parser.add_argument('access_key', metavar='YOUR_AWS_ACCESS_KEY', help='Your AWS access key ID')
  parser.add_argument('secret_key', metavar='YOUR_AWS_SECRET_KEY', help='Your AWS secret access key')
  args = parser.parse_args()

  set_cwd(__file__)
  import_boto3()
  ec2 = boto3.resource('ec2', region_name='us-west-2',  # us-west-2 has good pricing
      aws_access_key_id=args.access_key, aws_secret_access_key=args.secret_key)

  run(['chmod', '600', 'id_rsa'])
  create_secgroup(ec2, SEC_GROUP_NAME)
  run_instances(ec2, 3, args)
  setup_instances(ec2)
  cleanup()

def import_boto3():
  try:
    import boto3
  except ImportError:
    if not confirm('boto3 not installed. Do you want me to try and install it?'):
      abort()
    run(['pip', 'install', '--user', 'boto3'])
    import boto3
  globals()['boto3'] = __import__('boto3')

def create_secgroup(ec2, name):
  from botocore.exceptions import ClientError
  try:
    groups = list(ec2.security_groups.filter(GroupNames=[name]))
    if groups:
      highlight('Security group %s found, not recreating' % name)
      return
  except ClientError:
    pass

  highlight('Creating security group %s' % name)
  secgroup = ec2.create_security_group(GroupName=name, Description='Reproducing Spark results. Allows all TCP ingress/egress for simplicity.')
  secgroup.authorize_ingress(IpPermissions=[dict(IpProtocol='tcp',
      IpRanges=[dict(CidrIp='0.0.0.0/0')], FromPort=0, ToPort=65535)])
  secgroup.authorize_egress(IpPermissions=[dict(IpProtocol='tcp',
      IpRanges=[dict(CidrIp='0.0.0.0/0')], FromPort=0, ToPort=65535)])

def run_instances(ec2, n, args, check_running=True):
  global MASTER, SLAVES, IS_AWS_RUNNING
  if check_running:
    running = list(ec2.instances.filter(
          Filters=[{'Name': 'instance-state-name', 'Values': ['running']}]))
    # TODO also check running >= n
    if running:
      if not confirm('%d running instances found - do you want to try to use them instead of launching new instances?' % len(running)):
        running = None

  if not running:
    running = launch_and_wait_instances(ec2, n, args)

  IS_AWS_RUNNING = True
  master_id = None
  if not MASTER:
    master_id = running[0].id
    MASTER = list(ec2.instances.filter(InstanceIds=[master_id]))[0].public_dns_name
    ec2.create_tags(Resources=[master_id], Tags=[dict(Key='Name', Value='repro_rdd master')])
    highlight('Master is ' + MASTER)

  slave_instances = filter(lambda i: i.id != master_id, running)
  ec2.create_tags(Resources=[instance.id for instance in slave_instances], Tags=[dict(Key='Name', Value='repro_rdd')])
  slave_hosts = [instance.public_dns_name for instance in slave_instances]
  SLAVES += slave_hosts
  highlight('Added slaves are %r' % slave_hosts)

def launch_and_wait_instances(ec2, n, args):
  global IS_AWS_RUNNING
  if not confirm('About to launch %d instances. Proceed?' % n):
    sys.exit(1)

  instances = list(ec2.create_instances(ImageId=AMI_IMAGE_ID, MinCount=n, MaxCount=n,
      InstanceType=INSTANCE_TYPE, SecurityGroups=[SEC_GROUP_NAME]))
  IS_AWS_RUNNING = True
  highlight('Launched %r, waiting for initialization' % instances)

  while True:
    time.sleep(10)
    running = list(ec2.instances.filter(
        Filters=[{'Name': 'instance-state-name', 'Values': ['running']}]))
    print '%d instances running' % len(running)
    if len(running) == len(instances):
      break

  client = boto3.client('ec2', region_name='us-west-2',
      aws_access_key_id=args.access_key, aws_secret_access_key=args.secret_key)
  while True:
    statuses = client.describe_instance_status()
    for status in statuses['InstanceStatuses']:
      if status['InstanceStatus']['Status'] != 'ok':
        print 'Waiting for instances to finish initializing'
        continue
      break
    time.sleep(10)

  highlight('All launched instances are running')
  return running

def setup_instances(ec2):
  def write_core_site(fname, tmp_dir):
    with open('ec2/core-site.xml', 'w') as f:
      f.write("""<?xml version="1.0" encoding="UTF-8"?>
  <configuration>
   <property>
    <name>hadoop.tmp.dir</name>
    <value>%s</value>
    <description>A base for other temporary directories.</description>
   </property>

   <property>
    <name>fs.default.name</name>
    <value>hdfs://%s:54310</value>
    <description>The name of the default file system.  A URI whose
    scheme and authority determine the FileSystem implementation.  The
    uri's scheme determines the config property (fs.SCHEME.impl) naming
    the FileSystem implementation class.  The uri's authority is used to
    determine the host, port, etc. for a filesystem.</description>
   </property>
  </configuration>
  """ % (tmp_dir, MASTER))
  write_core_site('ec2/core-site.xml', '/home/ubuntu/hadoop_temp')
  write_core_site('ec2/core-site2.xml', '/home/ubuntu/hadoop_temp2')

  with open('ec2/mapred-site.xml', 'w') as f:
    f.write("""<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
 <property>
  <name>mapred.job.tracker</name>
  <value>%s:54311</value>
  <description>The host and port that the MapReduce job tracker runs
  at.  If "local", then jobs are run in-process as a single map
  and reduce task.
  </description>
 </property>
</configuration>
""" % MASTER)
  with open('ec2/slaves', 'w') as f:
    f.write('\n'.join(SLAVES))
  with open('ec2/masters', 'w') as f:
    f.write(MASTER)
  with open('ec2/mesos.conf', 'w') as f:
    f.write('master=%s:5050' % MASTER)

  highlight('Setting up machines')
  rsync_all(SLAVES + [MASTER], 'ec2/', '~/scripts')
  ssh_all(SLAVES, '~/scripts/run.sh')
  ssh(MASTER, 'touch ~/scripts/is_master; ~/scripts/run.sh')

def cleanup():
  global IS_AWS_RUNNING
  IS_AWS_RUNNING = False # TODO

if __name__ == '__main__':
  try:
    main()
  except KeyboardInterrupt:
    abort()
  finally:
    if IS_AWS_RUNNING:
      error('It seems an error occurred! Instances are likely STILL RUNNING. If '
            'you want to terminate them, you should go to the AWS console at '
            'https://us-west-2.console.aws.amazon.com/ec2/v2/home?region=us-west-2#Instances:sort=instanceState '
            'and terminate them manually.')
