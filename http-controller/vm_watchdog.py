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
from Queue import Queue

r = redis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT)

# Client flask, imports this and pushes vm instruction messages onto redis
class VmWatchDog:
    def __init__(self, vm_name):
        self.vm_name = vm_name

    def reset(self):
        r.rpush('vmwatchdog:win7workstation', json.dumps({
            'vm_name': self.vm_name,
            'action': 'reset'
        }))

    def restore(self):
        r.rpush('vmwatchdog:win7workstation', json.dumps({
            'vm_name': self.vm_name,
            'action': 'restore'
        }))

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
        self.queue_name = 'vmwatchdog:{}'.format(self.instance_name)

        self.threads = {}
        self.queues = {}

        for vm, snapshot in config.ACTIVE_VMS:
            self.queues[vm] = Queue()

        r.delete(self.queue_name)

    def run(self):
        try:
            self._run()
        except Exception as e:
            self.logger.error(e)

    def _run(self):
        for vm, snapshot in config.ACTIVE_VMS:
            self.logger.info('Starting vm mgr for {}'.format(vm))
            self.threads[vm] = VboxManager(vm, self.queues[vm])
            self.threads[vm].start()

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
            self.queues[vm_name].put('restore')


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

        self.logger.info('calling {}'.format(cmdline))

        if self.blocking:
            return subprocess.Popen(cmdline, shell=True).communicate()[0]
        else:
            subprocess.Popen(cmdline, shell=True)

    def restart(self):
        return self.call_ctrl_script('restart')

    def restore_snapshot(self):
        return self.call_ctrl_script('restore')

    def get_active_snapshot(self):
        for vm_s in config.ACTIVE_VMS:
            if self.vm_name == vm_s[0]:
                return vm_s[1]

    def run(self):
        try:
            self._run()
        except Exception as e:
            self.logger.error(e)

    def _run(self):
        while True:
            self.logger.info('polling... ({})'.format(self.queue.qsize()))
            msg = self.queue.get()
            self.restore_snapshot()


class VmHeartbeat(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.heartbeat_hash_name = 'heartbeat'
        self.curr_processing_hash_name = 'processing'
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
        idle_secs = (datetime.now() - datetime.fromtimestamp(float(idle_since))).seconds
        self.logger.info('{} idle for {}s'.format(vm, idle_secs))
        return idle_secs > self.timeout_secs_limit

    def create_bad_sample_metadata_json(self, sample, uuid, run_id):
        uri = 'http://192.168.1.145:5000/agent/error/{}/{}/{}'.format(sample, uuid, run_id)
        self.logger.info('calling {}'.format(uri))
        r = requests.post(uri, {'status':'ERR', 'status_msg':'vm watchdog timeout'})
        self.logger.info(r.text)

    def reset(self):
        for vm, snapshot in config.ACTIVE_VMS:
            self.set_processing(vm)
            self.heartbeat(vm)
            self.logger.info('reset vm heartbeat and processing {}'.format(vm))

    def run(self):
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

if __name__ == '__main__':

    for vm, snapshot in config.ACTIVE_VMS:
        VmWatchDog(vm).heartbeat()

    common.setup_logging('vm-watchdog.log')
    print 'starting heartbeat thread...'
    heartbeat_thread = VmHeartbeat().start()

    print 'starting cmd listener thread...'
    cmd_listener = VmWatchdogService().start()