import threading
import logging
import subprocess
import config
import json
from datetime import datetime
import time
import os
import redis
import common
import requests
from Queue import Queue, Empty
import psutil
from common import TimeoutExec
import getopt
import sys

r = redis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT)

# Client flask, imports this and pushes vm instruction messages onto redis
class VmWatchDog:
    def __init__(self, vm_name):
        self.vm_name = vm_name
        self.queue_key = 'vm_watchdog'
        self.queue_instance = 'win7workstation' # TODO: src from YAML

    def get_queue(self):
        return '{}:{}'.format(self.queue_key,
                              self.queue_instance)

    def reset(self):
        self.push({
            'vm_name': self.vm_name,
            'action': 'reset'
        })

    def restore(self):
        self.push({
            'vm_name': self.vm_name,
            'action': 'restore'
        })

    def push(self, dict):
        # Fire heartbeat to avoid race where action already queued
        self.heartbeat()
        r.rpush(self.get_queue(), json.dumps(dict))

    def heartbeat(self):
        VmHeartbeat().heartbeat(self.vm_name)

    def set_processing(self, sample='', uuid='', run_id=''):
        VmHeartbeat().set_processing(self.vm_name, sample, uuid, run_id)

    def clear_processing(self):
        VmHeartbeat().set_processing(self.vm_name)

    # TODO: This shouldn't be triggered from controller...
    @staticmethod
    def init():
        for vm ,snapshot in config.ACTIVE_VMS:
            VmWatchDog(vm).restore()


# Runs in daemon on machine to listen for vm instructions
class VmWatchdogService(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.instance_name = 'win7workstation'
        self.queue_name = 'vm_watchdog:{}'.format(self.instance_name)

        self.threads = {}
        self.queues = {}

        for vm, snapshot in config.ACTIVE_VMS:
            self.queues[vm] = Queue()

        r.delete(self.queue_name)

    def run(self, threading=False):
        try:
            self._run(threading)
        except Exception as e:
            self.logger.error(e)

    def init_worker_threads(self):
        for vm, snapshot in config.ACTIVE_VMS:
            self.logger.info('Starting vm mgr for {}'.format(vm))
            self.threads[vm] = VboxManager(vm, self.queues[vm])
            self.threads[vm].start()

    def _run(self, threading):
        if threading:
            self.init_worker_threads()

        self.logger.info('Entering polling loop...')
        while True:
            self.logger.info('polling...')

            raw = r.blpop(self.queue_name, 60)
            if raw is None:
                continue

            queue, msg = raw
            self.logger.info('recevied: {}'.format(msg))
            jmsg = json.loads(msg)

            vm_name = jmsg['vm_name']
            VmWatchDog(vm_name).clear_processing()

            if threading:
                self.queues[vm_name].put('restore')
            else:
                VboxManager(vm_name, None).restore_snapshot()


class VboxManager(threading.Thread):
    def __init__(self, vm_name, queue):
        threading.Thread.__init__(self)
        self.vm_name = vm_name
        self.script_path = 'bash ../vbox-controller/vbox-ctrl.sh'
        self.queue = queue
        self.blocking = True
        self.logger = logging.getLogger('{}_{}'.format(self.__class__.__name__, self.vm_name))

    def call_ctrl_script(self, action):
        cmdline = "{} -v '{}' -s '{}' -a '{}'".format(self.script_path, self.vm_name,
                                                      self.get_active_snapshot(), action)

        te = TimeoutExec(cmdline, 1)
        te.do_exec()

    def restart(self):
        return self.call_ctrl_script('restart')

    def restore_snapshot(self):
        return self.call_ctrl_script('restore')

    def get_active_snapshot(self):
        for vm_s in config.ACTIVE_VMS:
            if self.vm_name == vm_s[0]:
                return vm_s[1]

    def run(self):
        while True:
            try:
                self._run()
            except Exception as e:
                self.logger.error(e)

    def _run(self):
        while True:
            self.logger.info('polling... ({})'.format(self.queue.qsize()))
            try:
                msg = self.queue.get(timeout=15)
                self.restore_snapshot()
            except Empty as e:
                continue


class VmHeartbeat(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.heartbeat_hash_name = 'vm_watchdog:heartbeat'
        self.curr_processing_hash_name = 'vm_watchdog:processing'
        self.timeout_secs_limit = config.VM_HEARTBEAT_TIMEOUT_MINS * 60
        self.poll_tm_secs = 60
        self.logger = logging.getLogger(self.__class__.__name__)

    def heartbeat(self, vm_name):
        now = time.time()
        r.hset(self.heartbeat_hash_name, vm_name, now)
        self.logger.info('set heartbeat for {} -> {}'.format(vm_name, now))

    def set_processing(self, vm_name='', sample='', uuid='', run_id=''):
        r.hset(self.curr_processing_hash_name, vm_name, '{}:{}:{}'.format(sample, uuid, run_id))
        self.logger.info('set {} processing {}'.format(vm_name, sample))

    def get_last_processed(self, vm_name):
        return r.hget(self.curr_processing_hash_name, vm_name)

    def is_timeout_expired(self, vm):
        idle_since = r.hget(self.heartbeat_hash_name, vm)
        if idle_since is None or idle_since == '':
            return True

        idle_secs = (datetime.now() - datetime.fromtimestamp(float(idle_since))).seconds
        self.logger.info('{} idle for {}s'.format(vm, idle_secs))
        return idle_secs > self.timeout_secs_limit

    def create_bad_sample_metadata_json(self, sample, uuid, run_id):
        uri = '{}/agent/error/{}/{}/{}'.format(config.EXT_IF, sample, uuid, run_id)
        self.logger.info('calling {}'.format(uri))
        r = requests.post(uri, {'status':'ERR', 'status_msg':'vm watchdog timeout'})
        self.logger.info(r.text)

    def reset(self):
        for vm, snapshot in config.ACTIVE_VMS:
            self.set_processing(vm)
            self.heartbeat(vm)
            self.logger.info('reset vm heartbeat and processing {}'.format(vm))

    def _run(self):
        while True:
            for vm, snapshot in config.ACTIVE_VMS:
                if self.is_timeout_expired(vm):
                    self.logger.info('{} breached timeout limit, restoring...'.format(vm))
                    last_processing = self.get_last_processed(vm)
                    if last_processing != '::':
                        self.create_bad_sample_metadata_json(*last_processing.split(':'))
                        self.set_processing(vm)

                    VmWatchDog(vm).restore()

            time.sleep(self.poll_tm_secs)

    def run(self):
        while True:
            try:
                self._run()
            except Exception as e:
                self.logger.error(e)

if __name__ == '__main__':

    heartbeat_init = True
    opts, excess = getopt.getopt(sys.argv[1:], '', ['dont-init'])
    for opt, arg in opts:
        if opt in ('--dont-init'):
            heartbeat_init = False

    if heartbeat_init:
        for vm, snapshot in config.ACTIVE_VMS:
            VmWatchDog(vm).heartbeat()

    common.setup_logging('vm-watchdog.log')

    # TODO: this should probably know what VmWatchdogService is doing to avoid a race??
    print 'starting heartbeat thread...'
    heartbeat_thread = VmHeartbeat().start()

    print 'starting cmd listener thread...'
    cmd_listener = VmWatchdogService().start()