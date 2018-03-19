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

app = Flask(__name__)
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)

# TODO: Move this to redis
uuid_run_map = {}


#
# Consumer
#


@app.route("/consumer/get_extract/<sample>/<uuid>/<extract>")
def get_extract(sample, uuid, extract):
    return "OK"

#
# Agent Stub
#


@app.route('/agent-stub/get-agent')
def get_agent():
    return send_from_directory("../guest-agent/", "agent.py")

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
        if os.path.isdir(os.path.join('uploads', sample)):
            return True
        else:
            return False

    # TODO: this is not thread safe, using disk as master record
    def get_sample_to_process(self):
        to_process = filter(lambda x: self.sample_already_processed(x) is False,
                            os.listdir('samples'))

        if len(to_process) == 0:
            print 'No samples left to process'
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
        print '[{}] calling {}'.format("LIVE" if self.enabled else "MOCK", cmdline)

        if self.enabled:
            ret = subprocess.Popen(cmdline, shell=True)

    def restart(self):
        return self.call_ctrl_script('restart')

    def restore_snapshot(self):
        return self.call_ctrl_script('restore')


class VmHeartbeat(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.hash_name = 'heartbeat'
        self.timeout_secs_limit = 10 * 60
        self.poll_tm_secs = 60

    def heartbeat(self, vm_name):
        now = time.time()
        r.hset(self.hash_name, vm_name, now)
        print 'set heartbeat for {} -> {}'.format(vm_name, now)

    def run(self):
        while True:
            for vm, snapshot in ACTIVE_VMS:
                idle_since = r.hget(self.hash_name, vm)
                idle_secs = (datetime.now() - datetime.fromtimestamp(float(idle_since))).seconds
                print '{} idle for {}s'.format(vm, idle_secs)
                if idle_secs > self.timeout_secs_limit:
                    print '{} breached timeout limit, restoring...'.format(vm)
                    VmManager(vm, snapshot).restore_snapshot()
                    # TODO: create the upload dir for this sample - with node name incase its OS specific...
                    # how do i get this info?? reddiss???

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
        resp['sample_url'] = SampleQueue().get_sample_to_process()
        resp['action'] = 'init'

    resp['action'] = 'seek_and_destroy'

    # TODO: Remove this to return to normal flow
    resp['action'] = 'init'

    return json.dumps(resp)


@app.route('/agent/extractor_pack/<pack_name>')
def extractor_pack(pack_name):
    return send_from_directory("extractor-packs", "sample_pack.zip")


@app.route('/agent/upload/<sample>/<uuid>/<run_id>', methods=['GET', 'POST'])
def upload_results(sample, uuid, run_id, force_one_run=True):

    if sample.endswith('.exe'):
        sample = sample.replace('.exe', '')

    upload_dir = os.path.join('uploads', sample, uuid, run_id)

    try:
        os.makedirs(upload_dir)
    except:
        pass

    for f in request.files:
        local_f = os.path.join(upload_dir, f)
        request.files[f].save(local_f)
        print 'wrote {}'.format(local_f)

    metadata_f = 'run-{}-meta.json'.format(run_id)
    with open(os.path.join(upload_dir, metadata_f), 'w') as metadata:
        metadata.write(json.dumps(request.form))
        print 'wrote {}'.format(metadata_f)

    vm_mgr = VmManager(request.form['node'], 'autorun v0.2')

    if 'ERROR' in request.form.keys():
        print 'error in run: {}'.format(request.form['ERROR'])
        vm_mgr.restore_snapshot()
        return "OK"

    if int(request.form['runs-left']) <= 0 or force_one_run:
        print 'no runs left, restoring vm to snapshot'
        vm_mgr.restore_snapshot()
    else:
        print 'bouncing vm'
        vm_mgr.restart()

    print json.dumps(request.form)

    return "OK"


@app.route('/agent/get_sample/<sha256>')
def get_sample(sha256):
    return send_from_directory("samples", sha256)


def init(init_vms=True):
    if init_vms:
        for vm, snapshot in ACTIVE_VMS:
            vm_mgr = VmManager(vm, snapshot)
            vm_mgr.restore_snapshot()

    vm_hb = VmHeartbeat()
    vm_hb.start()

    app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024

    log_handler = TimedRotatingFileHandler('controller.log', when='D')
    log_handler.setLevel(logging.INFO)
    app.logger.addHandler(log_handler)


if __name__ == '__main__':
    init()
    app.run(host='0.0.0.0')
