import json
import uuid
import random
import os
import subprocess
import time
from datetime import datetime
from flask import Flask, request, send_from_directory
import redis
from threading import Thread
from config import *
import logging
from logging.handlers import TimedRotatingFileHandler
from consumer import *
import sys

app = Flask(__name__)
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)

# TODO: Move this to redis
uuid_run_map = {}


#
# Agent Stub
#


@app.route('/agent-stub/get-agent')
def get_agent():
    return send_from_directory(AGENT_DIR, "agent.py")

#
# Agent
#


def update_callback_details(cb_uuid):
    resp = {}
    if cb_uuid is None:
        cb_uuid = uuid.uuid4().hex
        uuid_run_map[cb_uuid] = 0
    elif cb_uuid in uuid_run_map:
        uuid_run_map[cb_uuid] += 1
    else:
        print 'WARN: received uuid we dont know about...'
        uuid_run_map[cb_uuid] = 0

    resp['run_id'] = uuid_run_map[cb_uuid]
    resp['uuid'] = cb_uuid

    return cb_uuid, resp


def get_form_param(param_name):
    if param_name in request.form:
        return request.form.get(param_name)
    else:
        return None


class SampleQueue:
    def sample_already_processed(self, sample):
        if os.path.isdir(os.path.join(UPLOADS_DIR, sample)):
            return True
        else:
            return False

    # TODO: this is not thread safe, using disk as master record
    def get_sample_to_process(self):
        to_process = filter(lambda x: self.sample_already_processed(x) is False,
                            os.listdir(SAMPLES_DIR))

        if len(to_process) == 0:
            app.logger.info('No samples left to process')
            return None

        print 'to process: {}'.format(len(to_process))
        return '{}/agent/get_sample/{}'.format(EXT_IF, random.choice(to_process))


class VmManager:
    def __init__(self, vm_name, snapshot_name, enabled=True):
        self.vm_name = vm_name
        self.snapshot_name = snapshot_name
        self.script_path = 'bash ../vbox-controller/vbox-ctrl.sh'
        self.enabled = enabled

    def call_ctrl_script(self, action):
        cmdline = "{} -v '{}' -s '{}' -a '{}'".format(self.script_path, self.vm_name,
                                                      self.snapshot_name, action)
        app.logger.info('[{}] calling {}'.format("LIVE" if self.enabled else "MOCK", cmdline))

        if self.enabled:
            return subprocess.Popen(cmdline, shell=True)

    def restart(self):
        return self.call_ctrl_script('restart')

    def restore_snapshot(self):
        return self.call_ctrl_script('restore')


class VmHeartbeat(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.heartbeat_hash_name = 'heartbeat'
        self.curr_processing_hash_name = 'processing'
        self.timeout_secs_limit = 10 * 60
        self.poll_tm_secs = 60
        self.logger = logging.getLogger(self.__class__.__name__)

    def heartbeat(self, vm_name):
        now = time.time()
        r.hset(self.heartbeat_hash_name, vm_name, now)
        self.logger.info('set heartbeat for {} -> {}'.format(vm_name, now))

    def set_processing(self, vm_name, sample):
        r.hset(self.curr_processing_hash_name, vm_name, sample)
        self.logger.info('set {} processing {}'.format(vm_name, sample))

    def get_last_processed(self, vm_name):
        return r.hget(self.curr_processing_hash_name, vm_name)

    def is_timeout_expired(self, vm):
        idle_since = r.hget(self.heartbeat_hash_name, vm)
        idle_secs = (datetime.now() - datetime.fromtimestamp(float(idle_since))).seconds
        self.logger.info('{} idle for {}s'.format(vm, idle_secs))
        return idle_secs > self.timeout_secs_limit

    def mark_bad_sample(self, vm, sample):
        udir = os.path.join(UPLOADS_DIR, sample)
        if not os.path.exists(udir):
            os.makedirs(udir)

        bad_f = os.path.join(udir, '{}.bad'.format(vm))
        with open(bad_f, 'w') as fd:
            fd.write('nop')

        self.logger.info('wrote {}'.format(bad_f))

    def reset(self):
        for vm, snapshot in ACTIVE_VMS:
            self.set_processing(vm, '')
            self.heartbeat(vm)
            self.logger.info('reset vm heartbeat and processing {}'.format(vm))

    def run(self):
        while True:
            for vm, snapshot in ACTIVE_VMS:
                if self.is_timeout_expired(vm):
                    self.logger.info('{} breached timeout limit, restoring...'.format(vm))
                    self.mark_bad_sample(vm, self.get_last_processed(vm))
                    VmManager(vm, snapshot).restore_snapshot()

            time.sleep(self.poll_tm_secs)


@app.before_request
def before_request():
    node = get_form_param('node')
    if node is not None:
        VmHeartbeat().heartbeat(node)


# TODO: Run_id & action needs to be done per extractor?!
@app.route('/agent/callback', methods=['POST'])
def callback():
    cb_uuid = get_form_param('uuid')
    node = get_form_param('node')

    cb_uuid, resp = update_callback_details(cb_uuid)
    print 'in callback - uuid: {} node: {}'.format(cb_uuid, node)

    resp['pack_url'] = EXTRACTOR_PACK_URL
    resp['pack'] = 'pack-1'

    if resp['run_id'] == 0:
        sample_to_process = SampleQueue().get_sample_to_process()
        VmHeartbeat().set_processing(node, sample_to_process.split('/')[-1])

        resp['sample_url'] = sample_to_process
        resp['action'] = 'init'

    resp['action'] = 'seek_and_destroy'

    # TODO: Remove this to return to normal flow
    resp['action'] = 'init'

    return json.dumps(resp)


@app.route('/agent/extractor_pack/<pack_name>')
def extractor_pack(pack_name):
    return send_from_directory(EXTRACTOR_PACK_DIR, "sample_pack.zip")


def get_upload_dir(*path_args):
    upload_dir = os.path.join(*path_args)
    try:
        os.makedirs(upload_dir)
    except:
        pass

    return upload_dir


# TODO: This is too big!
@app.route('/agent/upload/<sample>/<uuid>/<run_id>', methods=['GET', 'POST'])
def upload_results(sample, uuid, run_id, force_one_run=True):

    upload_dir = get_upload_dir(UPLOADS_DIR, sample.replace('.exe', ''), uuid, run_id)

    for f in request.files:
        local_f = os.path.join(upload_dir, f)
        request.files[f].save(local_f)
        app.logger.info('wrote {}'.format(local_f))

    metadata_f = 'run-{}-meta.json'.format(run_id)
    with open(os.path.join(upload_dir, metadata_f), 'w') as metadata:
        metadata.write(json.dumps(request.form))
        app.logger.info('wrote {}'.format(metadata_f))

    vm_mgr = VmManager(request.form['node'], 'autorun v0.2')

    if 'ERROR' in request.form.keys():
        app.logger.info('error in run: {}'.format(request.form['ERROR']))
        vm_mgr.restore_snapshot()
        return "OK"

    if int(request.form['runs-left']) <= 0 or force_one_run:
        app.logger.info('no runs left, restoring vm to snapshot')
        vm_mgr.restore_snapshot()
    else:
        app.logger.info('bouncing vm')
        vm_mgr.restart()

    print json.dumps(request.form)

    return "OK"


@app.route('/agent/get_sample/<sha256>')
def get_sample(sha256):
    return send_from_directory(SAMPLES_DIR, sha256)


def init(init_vms=True):
    setup_logging()
    if init_vms:
        for vm, snapshot in ACTIVE_VMS:
            vm_mgr = VmManager(vm, snapshot)
            vm_mgr.restore_snapshot()

    vm_hb = VmHeartbeat()
    vm_hb.reset()
    vm_hb.start()

    app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024


def setup_logging():
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    file_log_handler = TimedRotatingFileHandler('controller.log', when='D')
    file_log_handler.setLevel(logging.INFO)
    file_log_handler.setFormatter(formatter)

    console_log_handler = logging.StreamHandler(sys.stdout)
    console_log_handler.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(file_log_handler)
    root.addHandler(console_log_handler)

    app.logger.addHandler(file_log_handler)
    app.logger.addHandler(console_log_handler)


if __name__ == '__main__':
    init()
    logging.getLogger('main').info('starting...')
    app.run(host='0.0.0.0')
