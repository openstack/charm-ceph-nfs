#!/usr/bin/env python3
# Copyright 2021 OpenStack Charmers
# See LICENSE file for licensing details.

import json
import logging
import subprocess
import tempfile
import uuid

logger = logging.getLogger(__name__)


# TODO: Add ACL with client IPs
# TODO: Add ACL with kerberos
GANESHA_EXPORT_TEMPLATE = """EXPORT {{
    # Each EXPORT must have a unique Export_Id.
    Export_Id = {id};

    # The directory in the exported file system this export
    # is rooted on.
    Path = '{path}';

    # FSAL, Ganesha's module component
    FSAL {{
        # FSAL name
        Name = "Ceph";
        User_Id = "{user_id}";
        Secret_Access_Key = "{secret_key}";
    }}

    # Path of export in the NFSv4 pseudo filesystem
    Pseudo = '{path}';

    SecType = "sys";
    CLIENT {{
        Access_Type = "rw";
        Clients = 0.0.0.0;
    }}
    # User id squashing, one of None, Root, All
    Squash = "None";
}}
"""


class GaneshaNfs(object):

    export_index = "ganesha-export-index"
    export_counter = "ganesha-export-counter"

    def __init__(self, client_name, ceph_pool):
        self.client_name = client_name
        self.name = str(uuid.uuid4())
        self.ceph_pool = ceph_pool
        self.access_id = 'ganesha-{}'.format(self.name)

    def create_share(self):
        self.export_path = self._create_cephfs_share()
        export_id = self._get_next_export_id()
        export_template = GANESHA_EXPORT_TEMPLATE.format(
            id=export_id,
            path=self.export_path,
            user_id=self.access_id,
            secret_key=self._ceph_auth_key(),
        )
        logging.debug("Export template:: \n{}".format(export_template))
        tmp_file = self._tmpfile(export_template)
        self.rados_put('ganesha-export-{}'.format(export_id), tmp_file.name)
        self._ganesha_add_export(self.export_path, tmp_file.name)

    def _ganesha_add_export(self, export_path, tmp_path):
        return self._dbus_send(
            'ExportMgr', 'AddExport',
            'string:{}'.format(tmp_path), 'string:EXPORT(Path={})'.format(export_path))

    def _dbus_send(self, section, action, *args):
        cmd = [
            'dbus-send', '--print-reply', '--system', '--dest=org.ganesha.nfsd',
            '/org/ganesha/nfsd/{}'.format(section),
            'org.ganesha.nfsd.exportmgr.{}'.format(action)] + [*args]
        logging.debug("About to call: {}".format(cmd))
        return subprocess.check_output(cmd)

    def _create_cephfs_share(self):
        """Create an authorise a CephFS share.

        :returns: export path
        :rtype: union[str, bool]
        """
        try:
            self._ceph_subvolume_command('create', 'ceph-fs', self.name)
        except subprocess.CalledProcessError:
            logging.error("failed to create subvolume")
            return False

        try:
            self._ceph_subvolume_command(
                'authorize', 'ceph-fs', self.name,
                'ganesha-{name}'.format(name=self.name))
        except subprocess.CalledProcessError:
            logging.error("failed to authorize subvolume")
            return False

        try:
            output = self._ceph_subvolume_command('getpath', 'ceph-fs', self.name)
            return output.decode('utf-8').strip()
        except subprocess.CalledProcessError:
            logging.error("failed to get path")
            return False

    def _ceph_subvolume_command(self, *cmd):
        return self._ceph_fs_command('subvolume', *cmd)

    def _ceph_fs_command(self, *cmd):
        return self._ceph_command('fs', *cmd)

    def _ceph_auth_key(self):
        output = self._ceph_command(
            'auth', 'get', 'client.{}'.format(self.access_id), '--format=json')
        return json.loads(output.decode('UTF-8'))[0]['key']

    def _ceph_command(self, *cmd):
        cmd = ["ceph", "--id", self.client_name, "--conf=/etc/ceph/ganesha/ceph.conf"] + [*cmd]
        return subprocess.check_output(cmd)

    def _get_next_export_id(self):
        next_id = int(self.rados_get(self.export_counter))
        file = self._tmpfile(next_id + 1)
        self.rados_put(self.export_counter, file.name)
        return next_id

    def _tmpfile(self, value):
        file = tempfile.NamedTemporaryFile(mode='w+')
        file.write(str(value))
        file.seek(0)
        return file

    def rados_get(self, name):
        cmd = [
            'rados', '-p', self.ceph_pool, '--id', self.client_name,
            'get', name, '/dev/stdout'
        ]
        logging.debug("About to call: {}".format(cmd))
        output = subprocess.check_output(cmd)
        return output.decode('utf-8')

    def rados_put(self, name, source):
        cmd = [
            'rados', '-p', self.ceph_pool, '--id', self.client_name,
            'put', name, source
        ]
        logging.debug("About to call: {}".format(cmd))
        subprocess.check_call(cmd)
