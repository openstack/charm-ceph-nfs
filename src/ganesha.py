#!/usr/bin/env python3
# Copyright 2021 OpenStack Charmers
# See LICENSE file for licensing details.

import json
import logging
import subprocess
import tempfile
import uuid

logger = logging.getLogger(__name__)


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
        Clients = {clients};
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
        self.ceph_pool = ceph_pool

    def create_share(self, name=None, size=None):
        """Create a CephFS Share and export it via Ganesha

        :param name: String name of the share to create
        :param size: Int size in gigabytes of the share to create
        """
        if name is None:
            name = str(uuid.uuid4())
        if size is not None:
            size_in_bytes = size * 1024 * 1024
        access_id = 'ganesha-{}'.format(name)
        self.export_path = self._create_cephfs_share(name, size_in_bytes)
        export_id = self._get_next_export_id()
        export_template = GANESHA_EXPORT_TEMPLATE.format(
            id=export_id,
            path=self.export_path,
            user_id=access_id,
            secret_key=self._ceph_auth_key(access_id),
            clients='0.0.0.0'
        )
        logging.debug("Export template::\n{}".format(export_template))
        tmp_file = self._tmpfile(export_template)
        self._rados_put('ganesha-export-{}'.format(export_id), tmp_file.name)
        self._ganesha_add_export(self.export_path, tmp_file.name)
        self._add_share_to_index(export_id)
        return self.export_path

    def list_shares(self):
        pass

    def get_share(self, id):
        pass

    def update_share(self, id):
        pass

    def _ganesha_add_export(self, export_path, tmp_path):
        """Add a configured NFS export to Ganesha"""
        return self._dbus_send(
            'ExportMgr', 'AddExport',
            'string:{}'.format(tmp_path), 'string:EXPORT(Path={})'.format(export_path))

    def _dbus_send(self, section, action, *args):
        """Send a command to Ganesha via Dbus"""
        cmd = [
            'dbus-send', '--print-reply', '--system', '--dest=org.ganesha.nfsd',
            '/org/ganesha/nfsd/{}'.format(section),
            'org.ganesha.nfsd.exportmgr.{}'.format(action)] + [*args]
        logging.debug("About to call: {}".format(cmd))
        return subprocess.check_output(cmd)

    def _create_cephfs_share(self, name, size_in_bytes=None):
        """Create an authorise a CephFS share.

        :param name: String name of the share to create
        :param size_in_bytes: Integer size in bytes of the size to create

        :returns: export path
        :rtype: union[str, bool]
        """
        try:
            if size_in_bytes is not None:
                self._ceph_subvolume_command('create', 'ceph-fs', name, str(size_in_bytes))
            else:
                self._ceph_subvolume_command('create', 'ceph-fs', name)
        except subprocess.CalledProcessError:
            logging.error("failed to create subvolume")
            return False

        try:
            self._ceph_subvolume_command(
                'authorize', 'ceph-fs', name,
                'ganesha-{name}'.format(name=name))
        except subprocess.CalledProcessError:
            logging.error("failed to authorize subvolume")
            return False

        try:
            output = self._ceph_subvolume_command('getpath', 'ceph-fs', name)
            return output.decode('utf-8').strip()
        except subprocess.CalledProcessError:
            logging.error("failed to get path")
            return False

    def _ceph_subvolume_command(self, *cmd):
        """Run a ceph fs subvolume command"""
        return self._ceph_fs_command('subvolume', *cmd)

    def _ceph_fs_command(self, *cmd):
        """Run a ceph fs command"""
        return self._ceph_command('fs', *cmd)

    def _ceph_auth_key(self, access_id):
        """Retrieve the CephX key associated with this id

        :returns: The access key
        :rtype: str
        """
        output = self._ceph_command(
            'auth', 'get', 'client.{}'.format(access_id), '--format=json')
        return json.loads(output.decode('UTF-8'))[0]['key']

    def _ceph_command(self, *cmd):
        """Run a ceph command"""
        cmd = ["ceph", "--id", self.client_name, "--conf=/etc/ceph/ceph.conf"] + [*cmd]
        return subprocess.check_output(cmd, stderr=subprocess.DEVNULL)

    def _get_next_export_id(self):
        """Retrieve the next available export ID, and update the rados key

        :returns: The export ID
        :rtype: str
        """
        next_id = int(self._rados_get(self.export_counter))
        file = self._tmpfile(next_id + 1)
        self._rados_put(self.export_counter, file.name)
        return next_id

    def _tmpfile(self, value):
        file = tempfile.NamedTemporaryFile(mode='w+')
        file.write(str(value))
        file.seek(0)
        return file

    def _rados_get(self, name):
        """Retrieve the content of the RADOS object with a given name
        
        :param name: Name of the RADOS object to retrieve

        :returns: Contents of the RADOS object
        :rtype: str
        """
        cmd = [
            'rados', '-p', self.ceph_pool, '--id', self.client_name,
            'get', name, '/dev/stdout'
        ]
        logging.debug("About to call: {}".format(cmd))
        output = subprocess.check_output(cmd)
        return output.decode('utf-8')

    def _rados_put(self, name, source):
        """Store the contents of the source file in a named RADOS object.
        
        :param name: Name of the RADOS object to retrieve
        :param source: Path to a file to upload to RADOS.
        
        :returns: None
        """
        cmd = [
            'rados', '-p', self.ceph_pool, '--id', self.client_name,
            'put', name, source
        ]
        logging.debug("About to call: {}".format(cmd))
        subprocess.check_call(cmd)

    def _add_share_to_index(self, export_id):
        index = self._rados_get(self.export_index)
        index += '%url rados://{}/ganesha-export-{}'.format(self.ceph_pool, export_id)
        tmpfile = self._tmpfile(index)
        self._rados_put(self.export_index, tmpfile.name)
