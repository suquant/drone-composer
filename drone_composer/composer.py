#!/usr/bin/env python

import os
import re
import sys
import tempfile

import drone

from snapshot import RemoteManager
from snapshot import SnapshotManager


def main():
    payload = drone.plugin.get_input()
    project_workspace = payload.get('workspace', {})
    private_key = project_workspace.get('keys', {}).get('private')
    if private_key:
        id_rsa_path = '/root/.ssh/id_rsa'
        os.mkdir(os.path.dirname(id_rsa_path))
        f = open(id_rsa_path, 'w+')
        f.write(private_key)
        f.close()
        os.chmod(id_rsa_path, 0700)
    vargs = payload["vargs"]
    environments = vargs.get('environments', {})
    ref = payload.get('build', {}).get('ref')
    name = vargs.get('name')
    if not name:
        name = ref.split('/')[-1]
    cleaned_name = ''.join(map(lambda w: w if w in re._alphanum else '-', name))
    environments['NAME'] = cleaned_name
    snapshot = vargs.get('snapshot')
    if snapshot:
        snapshot_manager = SnapshotManager(snapshot.get('ssh'), snapshot.get('image'))
        device = snapshot.get('device')
        size = snapshot.get('size')
        try:
            sys.stdout.write('try down snapshot for "{}" -> "{}"{}'.format(
                device, cleaned_name, os.linesep))
            snapshot_manager.down(device, cleaned_name)
        except Exception as e:
            sys.stderr.write(u'error: {}{}'.format(unicode(e), os.linesep))
            pass
        snapshot_environment_label = snapshot.get('environment_label', 'SNAPSHOT_NFS_MOUNTCOMMAND')
        environments[snapshot_environment_label] = snapshot_manager.up(device, cleaned_name, size)
    remote = RemoteManager(vargs.get('destination'))
    compose_file = os.path.join(project_workspace.get('path', ''), vargs.get('file'))
    dst_dir = os.path.join('.drone-composer', cleaned_name)
    environments['COMPOSE_NAME'] = ''.join(map(lambda w: w if w in re._alphanum else '', cleaned_name))
    environments['WORKSPACE'] = os.path.join(os.sep, 'home', 'core', dst_dir)
    compose_file_dst = os.path.join(dst_dir, 'docker-compose.yml')
    rsync = remote.rsync(compose_file, compose_file_dst, mkdir=True)
    if rsync.wait() != 0:
        sys.stderr.writelines(rsync.stderr.readlines())
    service_file_dst = os.path.join(dst_dir, '{}.service'.format(cleaned_name))
    service_template_file = os.path.join(project_workspace.get('path', ''),
                                         vargs.get('service', {}).get('template_file'))
    with tempfile.NamedTemporaryFile() as tmp:
        service_content = open(service_template_file).read()
        for k, v in environments.items():
            service_content = service_content.replace('{{{{{}}}}}'.format(k), v)
        tmp.write(service_content)
        tmp.flush()
        rsync = remote.rsync(tmp.name, service_file_dst)
        if rsync.wait() != 0:
            sys.stderr.writelines(rsync.stderr.readlines())
    command = 'cd {dst} && fleetctl start {service_name}'.format(
        dst=dst_dir, service_name='{}.service'.format(cleaned_name))
    ssh = remote.ssh(command)
    if ssh.wait() != 0:
        sys.stderr.writelines(ssh.stderr.readlines())


if __name__ == "__main__":
    main()
