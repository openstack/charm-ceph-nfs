# ceph-nfs

## Description

CephNFS is a charm designed to enable management of NFS shares backed
by CephFS. It supports Ceph Pacific and above.

## Usage

CephNFS provides an additional service when deployed with Ceph and CephFS.
It should be related to CephMon:

    juju add-relation ceph-nfs:ceph-client ceph-mon:client

Once all relations have settled, it is possible to create a new export:

    juju run-action --wait ceph-nfs/0 create-share name=test-share size=10 allowed-ips=10.0.0.0/24

The above command has creates an NFS share that is 10GB in size, and is
accessible from any machine in the 10.0.0.0-10.0.0.255 network space. To
grant access to a new network address, the `grant-access` action should be
used:

    juju run-action --wait ceph-nfs/0 grant-access name=test-share allowed-ips=192.168.0.10 mode=r

This command has granted read-only access to the named share to a specific
address: `192.168.0.1`.

## Relations

TODO: Provide any relations which are provided or required by your charm

## Contributing

Please see the [Juju SDK docs](https://juju.is/docs/sdk) for guidelines 
on enhancements to this charm following best practice guidelines, and
`CONTRIBUTING.md` for developer guidance.
