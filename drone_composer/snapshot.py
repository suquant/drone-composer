#!/usr/bin/env python

import argparse
import copy
import hashlib
import json
import logging
import os
import re
import subprocess
import sys
from urlparse import urlparse

logger = logging.getLogger()


class RemoteManager(object):
    def __init__(self, ssh):
        self.ssho = urlparse(ssh)

    def ssh(self, command, sudo=None):
        if sudo:
            command = "sudo " + command
        commands = [
            "ssh", "-o", "StrictHostKeyChecking=no",
            "-p", str(self.ssho.port or 22),
            "{}@{}".format(self.ssho.username, self.ssho.hostname), command
        ]
        sys.stdout.write('execute: {}{}'.format(' '.join(commands), os.linesep))
        return subprocess.Popen(
            commands, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def rsync(self, src, dst, mkdir=None, sudo=None):
        if mkdir:
            ssh = self.ssh('mkdir -p {}'.format(os.path.dirname(dst)))
            if ssh.wait() != 0:
                sys.stderr.writelines(ssh.stderr.readlines())
        commands = [
            'rsync', '-a', '--port={}'.format(self.ssho.port or 22),
            'ssh -o StrictHostKeyChecking=no', src,
            '{}@{}:{}'.format(self.ssho.username, self.ssho.hostname, dst)
        ]
        sys.stdout.write('execute: {}{}'.format(' '.join(commands), os.linesep))
        return subprocess.Popen(
            commands, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


    def is_path_exist(self, path):
        ssh = self.ssh('ls {}'.format(path))
        return ssh.wait() == 0


class SnapshotManager(RemoteManager):
    def __init__(self, ssh, image):
        super(SnapshotManager, self).__init__(ssh)
        self.image = image

    def create(self, device, name, size):
        device_paths = device.split(os.sep)
        device_paths[-1] = name
        device_path = str(os.sep).join(device_paths)
        if self.is_path_exist(device_path):
            return device_path
        command = 'lvcreate --size {size} --snapshot --name {name} {device}'.format(
            size=size, name=name, device=device)
        ssh = self.ssh(command, sudo=True)
        if ssh.wait() != 0:
            sys.stderr.writelines(ssh.stderr.readlines())
        return device_path

    def remove(self, device):
        if not self.is_path_exist(device):
            return device
        attributes = self.attributes(device)
        if attributes[0].lower() != 's':
            msg = '{device}: can not remove not snapshot volume'.format(
                device=device)
            sys.stderr.write(msg + os.linesep)
            raise Exception(msg)
        command = 'lvremove --force {}'.format(device)
        ssh = self.ssh(command, sudo=True)
        if ssh.wait() != 0:
            sys.stderr.writelines(ssh.stderr.readlines())
            raise Exception('Cannot remove snapshot: {}'.format(device))
        return device

    def attributes(self, device):
        command = 'lvs -o lv_attr {}'.format(device)
        ssh = self.ssh(command, sudo=True)
        if ssh.wait() != 0:
            sys.stderr.writelines(ssh.stderr.readlines())
            raise Exception('{}: attributes retrive error'.format(device))
        ssh.stdout.readline()
        return ssh.stdout.readline().strip()

    def lsblk(self, device):
        columns = ('NAME', 'UUID', 'MOUNTPOINT', 'FSTYPE', 'STATE', 'SIZE', 'TYPE')
        command = 'lsblk {device} -o {columns} --pairs'.format(
            device=device, columns=','.join(columns))
        ssh = self.ssh(command)
        if ssh.wait() != 0:
            sys.stderr.writelines(ssh.stderr.readlines())
            raise Exception('Can not get information about device: {}'.format(device))
        line = ssh.stdout.readline()
        result = {}
        for k, v in re.findall(r'([A-Za-z0-9\-]+)\=\"(.*?)\"', line):
            result[k.lower()] = v
        return result

    def df(self, path):
        columns = ('source', 'fstype', 'size', 'used', 'avail', 'target')
        command = 'df --sync --output={columns} {path}'.format(
            columns=','.join(columns), path=path)
        ssh = self.ssh(command)
        if ssh.wait() != 0:
            sys.stderr.writelines(ssh.stderr.readlines())
            raise Exception('Can not get df about path: {}'.format(path))
        # skip first line
        ssh.stdout.readline()
        line = ssh.stdout.readline()
        return dict(zip(columns, line.split()))

    def is_mountpoint(self, path):
        command = 'mountpoint -q {}'.format(path)
        ssh = self.ssh(command)
        return ssh.wait() == 0

    def mkdir(self, path):
        command = 'mkdir -p {}'.format(path)
        ssh = self.ssh(command, sudo=True)
        if ssh.wait() != 0:
            sys.stderr.writelines(ssh.stderr.readlines())
            raise Exception('Cannot create directory: {}'.format(path))
        return path

    def mount(self, device, path):
        lsblk_info = self.lsblk(device)
        mountpoint = lsblk_info['mountpoint']
        if mountpoint and mountpoint != path:
            msg = '{device}: already mounted to "{mountpoint}:. Can not mount to "{path}"'.format(
                mountpoint=mountpoint, path=path, device=device)
            sys.stderr.write(msg + os.linesep)
            raise Exception(msg)
        if not self.is_path_exist(device):
            raise Exception('device: {} not exists'.format(device))
        if not self.is_path_exist(path):
            self.mkdir(path)
        if self.is_mountpoint(path):
            df = self.df(path)
            path_device_name = df['source'].split(os.sep)[-1]
            target_device_name = lsblk_info['name']
            if path_device_name != target_device_name:
                msg = '{path}: already mounted to "{path_device_name}". Can not mount to "{target_device_name}"'.format(
                    path=path, path_device_name=path_device_name, target_device_name=target_device_name)
                sys.stderr.write(msg + os.linesep)
                raise Exception(msg)
            return path
        command = 'mount -o nouuid {device} {path}'.format(device=device, path=path)
        ssh = self.ssh(command, sudo=True)
        if ssh.wait() != 0:
            sys.stderr.writelines(ssh.stderr.readlines())
            raise Exception('Cannot mount device "{}" to "{}"'.format(device, path=path))
        return path

    def umount(self, path, force=None, lazy=None):
        command = 'umount'
        if force:
            command += ' --force'
        if lazy:
            command += ' --lazy'
        command += ' ' + path
        ssh = self.ssh(command, sudo=True)
        if ssh.wait() != 0:
            sys.stderr.writelines(ssh.stderr.readlines())
            raise Exception('{path}: can not unmount'.format(path=path))
        return path

    def inspect(self, name):
        command = 'docker inspect {}'.format(name)
        ssh = self.ssh(command)
        if ssh.wait() != 0:
            return None
        return json.load(ssh.stdout)[0]

    def get_container_name(self, volume_path):
        return hashlib.sha256(volume_path).hexdigest()[:10]

    def run_gluster(self, name, volume_path):
        container_path = '/data'
        image = self.image
        inspect = self.inspect(name)
        if not inspect:
            commands = [
                'docker run', '--detach', '--restart=always', '--name={}'.format(name),
                '--cap-add=SYS_ADMIN', '--cap-add=MKNOD', '--device=/dev/fuse', '--hostname={}'.format(name),
                '--dns=$(ifconfig docker0 | grep \'inet \' | awk -F\' \'  \'{print $2}\' | awk \'{print $1}\')',
                '--volume=/opt/var/lib/glusterd-{}:/var/lib/glusterd'.format(name),
                '--volume={}:{}'.format(volume_path, container_path), image
            ]
            command = ' '.join(commands)
            ssh = self.ssh(command)
            if ssh.wait() != 0:
                sys.stderr.writelines(ssh.stderr.readlines())
                raise Exception('Cannot run "{}" image'.format(image))
            self.initialize_gluster_volume(name, container_path)
        return name

    def initialize_gluster_volume(self, container, path):
        commands = [
            #'docker exec {container} mv {path}/.glusterfs {path}/.origin_glusterfs'.format(container=container, path=path),
            'docker exec {container} gluster volume create {container} {container}:{path} force'.format(
                container=container, path=path),
            'docker exec {container} gluster volume start {container}'.format(container=container)
        ]
        for command in commands:
            ssh = self.ssh(command)
            if ssh.wait() != 0:
                sys.stderr.writelines(ssh.stderr.readlines())
                break

    def stop_gluster(self, container):
        commands = [
            'docker stop --time=10 {}'.format(container),
            'docker rm {}'.format(container),
            'rm -rf /opt/var/lib/glusterd-{}'.format(container)
        ]
        for command in commands:
            ssh = self.ssh(command, sudo=True)
            if ssh.wait() != 0:
                sys.stderr.writelines(ssh.stderr.readlines())

    def nfs_credentials(self, container):
        inspect = self.inspect(container)
        options = ('rw', 'nofail', 'noauto', 'nolock', 'soft', 'noatime', 'timeo=50')
        return {'options': options, 'host': inspect['NetworkSettings']['IPAddress'],
                'path': '/{}'.format(container)}

    def nfs_mount_command(self, container):
        credentials = self.nfs_credentials(container)
        return 'mount -t nfs -o {options} {host}:{source}'.format(
            options=','.join(credentials['options']), host=credentials['host'],
            source=credentials['path'])

    def up(self, device, name, size):
        snapshot_device = self.create(device, name, size)
        snapshot_path = self.mount(snapshot_device, '/mnt/{}'.format(name))
        container = self.run_gluster(name, snapshot_path)
        return self.nfs_mount_command(container)

    def down(self, device, name):
        snapshot_device = os.sep.join(device.split(os.sep)[:-1] + [name])
        self.stop_gluster(name)
        self.umount(snapshot_device)
        self.remove(snapshot_device)


def main():
    parser = argparse.ArgumentParser(description='Snapshot management tool.')

    parser.add_argument(
        '-ssh', '--ssh', dest='ssh', type=str, required=True,
        help='Volume host, example: //core@st01.example.com:22/')
    parser.add_argument(
        '-image', '--image', dest='image', type=str,
        default='suquant/glusterd:3.6.9.1', help='Gluster image')

    parser.add_argument(
        '-log-level', '--log-level', dest='log_level', type=str, default='error',
        help='Log level')

    parser.add_argument('args', nargs='+')

    args = parser.parse_args()
    logger.setLevel(args.log_level.upper())
    manager = SnapshotManager(args.ssh, args.image)
    arguments = copy.copy(args.args)
    result = getattr(manager, arguments[0])(*arguments[1:])
    if isinstance(result, basestring):
        sys.stdout.write(result + os.linesep)
    elif isinstance(result, subprocess.Popen):
        if result.wait() != 0:
            sys.stderr.writelines(result.stderr.readlines())
        else:
            sys.stdout.writelines(result.stdout.readlines())


if __name__ == '__main__':
    main()
