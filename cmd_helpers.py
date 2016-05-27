from distutils.util import strtobool
import subprocess
import sys
import os

SSH_BATCH_SIZE = 10

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
  print ' '.join(cmd)
  subprocess.check_call(cmd)

def ssh(host, cmd):
  # For simplicity, we include private key as part of repo - probably not a best
  # practice, but we want this to be as easy to run as possible.
  run(_ssh_command(host, cmd))

def _ssh_command(host, cmd):
  return ['ssh', '-o', 'StrictHostKeyChecking=no', '-o', 'UserKnownHostsFile=/dev/null', '-i', 'id_rsa', 'ubuntu@' + host, cmd]

def rsync(host, dirpath, remotepath):
  run(_rsync_command(host, dirpath, remotepath))

def _rsync_command(host, dirpath, remotepath):
  return ['rsync', '-az', '--delete', '-e',
      'ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i id_rsa',
      dirpath, 'ubuntu@%s:%s' % (host, remotepath)]

def run_all(hosts, cmdfunc):
  i = 0
  while True:
    procs = []
    for _ in range(SSH_BATCH_SIZE):
      if i >= len(hosts):
        break
      cmd = cmdfunc(hosts[i])
      print ' '.join(cmd)
      p = subprocess.Popen(cmd)
      p.cmd = cmd
      procs.append(p)
      i += 1

    for j, ret in enumerate(map(lambda p: p.wait(), procs)):
      if ret:
        raise subprocess.CalledProcessError(ret, procs[j].cmd)
    if i >= len(hosts):
      return

def ssh_all(hosts, cmd):
  run_all(hosts, lambda h: _ssh_command(h, cmd))

def rsync_all(hosts, dirpath, remotepath):
  run_all(hosts, lambda h: _rsync_command(h, dirpath, remotepath))
