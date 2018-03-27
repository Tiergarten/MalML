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
import sys
from common import *
from vm_watchdog import VmWatchDog

app = Flask(__name__)

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
                            get_samples(SAMPLES_DIR))

        if len(to_process) == 0:
            app.logger.warn('No samples left to process...')
            return None

        print 'to process: {}'.format(len(to_process))
        return '{}/agent/get_sample/{}'.format(EXT_IF, random.choice(to_process))

@app.before_request
def before_request():
    node = get_form_param('node')
    if node is not None:
        VmWatchDog(node).heartbeat()


# TODO: Run_id & action needs to be done per extractor?!
@app.route('/agent/callback', methods=['POST'])
def callback():
    cb_uuid = get_form_param('uuid')
    node = get_form_param('node')

    cb_uuid, resp = update_callback_details(cb_uuid)
    print 'in callback - uuid: {} node: {}'.format(cb_uuid, node)

    resp['pack_url'] = EXTRACTOR_PACK_URL
    resp['extractor-pack'] = 'pack-1'

    if resp['run_id'] == 0:
        sample_to_process = SampleQueue().get_sample_to_process()
        if sample_to_process is None:
            return "OK"

        sample_sha = sample_to_process.split('/')[-1].replace('.exe', '')
        VmWatchDog(node).set_processing(sample_sha, cb_uuid, 0)

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


def get_md_fn(run_id):
    return 'run-{}-meta.json'.format(run_id)


def vm_action(vm_name, action):
    if SampleQueue().get_sample_to_process() is None:
        return

    if action == 'restore':
        VmWatchDog(vm_name).restore()
    elif action == 'reset':
        VmWatchDog(vm_name).rest()


# TODO: This is tOoOooo big!
@app.route('/agent/upload/<sample>/<uuid>/<run_id>', methods=['GET', 'POST'])
def upload_results(sample, uuid, run_id, force_one_run=True):
    upload_dir = get_upload_dir(UPLOADS_DIR, sample.replace('.exe', ''), uuid, run_id)

    for f in request.files:
        local_f = os.path.join(upload_dir, f)
        request.files[f].save(local_f)
        app.logger.info('wrote {}'.format(local_f))

    metadata_f = get_md_fn(run_id)
    with open(os.path.join(upload_dir, metadata_f), 'w') as metadata:
        metadata.write(json.dumps(request.form, indent=4, sort_keys=True))
        app.logger.info('wrote {}'.format(metadata_f))

    node_nm = get_form_param('node')
    if node_nm is None:
        app.logger.error('no node name')
        return "ERR - no node name"

    vm_name = get_form_param('node')

    VmWatchDog(vm_name).clear_processing()

    if not get_form_param('status') in ['OK', 'WARN']:
        app.logger.info('error in run: {}:{}'.format(request.form['status'], request.form['status_msg']))
        vm_action(vm_name, 'restore')
        return "OK"

    if int(request.form['runs-left']) <= 0 or force_one_run:
        app.logger.info('no runs left, restoring vm to snapshot')
        vm_action(vm_name, 'restore')
    else:
        app.logger.info('bouncing vm')
        vm_action(vm_name, 'reset')

    return "OK"


@app.route('/agent/error/<sample>/<uuid>/<run_id>', methods=['GET', 'POST'])
def agent_error(sample, uuid, run_id):
    upload_dir = get_upload_dir(UPLOADS_DIR, sample.replace('.exe', ''), uuid, run_id)

    md = {}
    set_run_status(md, get_form_param('status'), get_form_param('status_msg'))
    metadata_f = get_md_fn(run_id)
    with open(os.path.join(upload_dir, metadata_f), 'w') as metadata:
        metadata.write(json.dumps(request.form, indent=4, sort_keys=True))
        app.logger.info('wrote {}'.format(metadata_f))

    app.logger.info('wrote error file {}'.format(metadata_f))
    return "OK"


@app.route('/agent/get_sample/<sha256>')
def get_sample(sha256):
    return send_from_directory(SAMPLES_DIR, sha256)


def init():
    file_log_handler, console_log_handler = setup_logging('controller.log')
    app.logger.addHandler(file_log_handler)
    app.logger.addHandler(console_log_handler)

    app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024
    VmWatchDog.init()


if __name__ == '__main__':
    init()
    logging.getLogger('main').info('starting...')
    app.run(host='0.0.0.0')
