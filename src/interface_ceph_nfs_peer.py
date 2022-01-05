#!/usr/bin/env python3

# import json
import logging
# import socket

from ops.framework import (
    StoredState,
    EventBase,
    ObjectEvents,
    EventSource,
    Object)


class PoolInitialisedEvent(EventBase):
    pass


class CephNfsPeerEvents(ObjectEvents):
    pool_initialised = EventSource(PoolInitialisedEvent)


class CephNfsPeers(Object):

    on = CephNfsPeerEvents()
    _stored = StoredState()

    def __init__(self, charm, relation_name):
        super().__init__(charm, relation_name)
        self.relation_name = relation_name
        self.this_unit = self.framework.model.unit
        self._stored.set_default(
            pool_initialised=False)
        self.framework.observe(
            charm.on[relation_name].relation_changed,
            self.on_changed)

    def on_changed(self, event):
        logging.info("CephNfsPeers on_changed")
        if self.pool_initialised == 'True' and not self._stored.pool_initialised:
            self.on.pool_initialised.emit()
        self._stored.pool_initialised = True

    def pool_initialised(self):
        logging.info("Setting pool initialised")
        self.peer_rel.data[self.peer_rel.app]['pool_initialised'] = 'True'
        self.on.pool_initialised.emit()

    @property
    def peer_rel(self):
        return self.framework.model.get_relation(self.relation_name)
