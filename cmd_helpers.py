from distutils.util import strtobool
import subprocess
import sys
import os

# Sets working directory to parent of f
def set_cwd(f):
  abspath = os.path.abspath(f)
  dname = os.path.dirname(abspath)
  os.chdir(dname)

# Print a green message, ending with a newline or space if `newline` is False.
def highlight(msg, newline=True):
  print '\033[92m' + msg + '\033[0m',
  if newline:
    print

def error(msg, newline=True):
  print '\033[31m' + msg + '\033[0m',
  if newline:
    print

def confirm(prompt):
  while True:
    highlight(prompt + ' [y/n]', False)
    response = raw_input()
    try:
      return strtobool(response)
    except ValueError:
      print 'Invalid response, try again'

def abort():
  print 'Abandon ship!'
  sys.exit(1)

def run(cmd, msg=None):
  if msg:
    highlight(msg)
  try:
    print ' '.join(cmd)
    subprocess.check_call(cmd)
  except subprocess.CalledProcessError:
    print >> sys.stderr, 'Error running ' + repr(cmd) + '\n\nStack trace:'
    raise

def ssh(host, cmd):
  # For simplicity, we include private key as part of repo - probably not a best
  # practice, but we want this to be as easy to run as possible.
  run(['ssh', '-o', 'StrictHostKeyChecking=no', '-o', 'UserKnownHostsFile=/dev/null', '-i', 'id_rsa', 'ubuntu@' + host, cmd])

def ssh_async(host, cmd):
  ssh(host, 'nohup ' + cmd)

def rsync(host, dirpath, remotepath):
  run(['rsync', '-az', '--delete', '-e', 'ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i id_rsa',
      dirpath, 'ubuntu@%s:%s' % (host, remotepath)])
