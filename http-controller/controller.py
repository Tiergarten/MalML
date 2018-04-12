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
from vm_watchdog.vm_watchdog import VmWatchDog

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
        app.logger.warn('received uuid we dont know about...')
        uuid_run_map[cb_uuid] = 0

    resp['run_id'] = uuid_run_map[cb_uuid]
    resp['uuid'] = cb_uuid

    return cb_uuid, resp


def get_form_param(param_name):
    if param_name in request.form:
        return request.form.get(param_name)
    else:
        return None


class UploadPublisher:
    @staticmethod
    def publish(detonation_upload, run_id):
        detonation_upload_to_es(detonation_upload, run_id)
        UploadPublisher.publish_to_redis(detonation_upload, run_id)

    @staticmethod
    def publish_to_redis(du, run_id):
        return ReliableQueue(config.REDIS_UPLOAD_QUEUE_NAME).enqueue(du.to_json(run_id))


@app.before_request
def before_request():
    node = get_form_param('node')
    if node is not None:
        VmWatchDog(node).heartbeat()


def get_extractor_pack_url(pack_name):
    return EXTRACTOR_PACK_URL


# TODO: Run_id & action needs to be done per extractor?!
@app.route('/agent/callback', methods=['POST'])
def callback():
    cb_uuid = get_form_param('uuid')
    node = get_form_param('node')

    cb_uuid, resp = update_callback_details(cb_uuid)
    app.logger.info('in callback - uuid: {} node: {}'.format(cb_uuid, node))

    if resp['run_id'] == 0 or True:
        sample_metadata = SampleQueue().get_sample_to_process(node)
        if sample_metadata is None:
            app.logger.info('No samples left to process, returning...')
            return "OK"

        resp['sample_url'] = '{}/agent/get_sample/{}'.format(EXT_IF, sample_metadata['sample'])
        resp['action'] = 'init{}'.format(sample_metadata['arch'])
        resp['extractor-pack'] = sample_metadata['extractor-pack']
        resp['pack_url'] = get_extractor_pack_url(sample_metadata['extractor-pack'])
    else:
        resp['action'] = 'seek_and_destroy'

    return json.dumps(resp)


@app.route('/agent/extractor_pack/<pack_name>')
def extractor_pack(pack_name):
    return send_from_directory(EXTRACTOR_PACK_DIR, "sample_agg.zip")


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
    if SampleQueue().get_sample_to_process(vm_name) is None:
        return

    if action == 'restore':
        VmWatchDog(vm_name).restore()
    elif action == 'reset':
        VmWatchDog(vm_name).rest()


@app.route('/agent/upload/<_sample>/<uuid>/<run_id>', methods=['GET', 'POST'])
def upload_results(_sample, uuid, run_id, force_one_run=True):
    sample = _sample.replace('.exe', '')

    du = DetonationUpload(UPLOADS_DIR, sample, uuid, [run_id])

    for f in request.files:
        request.files[f].save(du.get_path(f))
        app.logger.info('wrote {}'.format(du.get_path(f)))

    du.write_metadata(json.dumps(request.form, indent=4, sort_keys=True))
    app.logger.info('wrote {}'.format(du.get_metadata_path(run_id)))

    UploadPublisher.publish(du, run_id)

    vm_name = get_form_param('node')
    if vm_name is None:
        app.logger.error('no node name')
        return "ERR - no node name"

    SampleQueue().set_processed(vm_name, sample)

    if not get_form_param('status') in ['OK', 'WARN']:
        app.logger.error('error in run: {}:{}'.format(request.form['status'], request.form['status_msg']))
        vm_action(vm_name, 'restore')

    elif int(request.form['runs-left']) <= 0 or force_one_run:
        app.logger.info('no runs left, restoring vm to snapshot')
        vm_action(vm_name, 'restore')

    else:
        app.logger.info('bouncing vm')
        vm_action(vm_name, 'reset')

    return "OK"


@app.route('/agent/error/<sample>', methods=['GET', 'POST'])
def agent_error(sample):
    run_id = str(0)
    upload_dir = get_upload_dir(UPLOADS_DIR, sample.replace('.exe', ''), uuid.uuid4().hex, run_id)

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


if __name__ == '__main__':
    init()
    logging.getLogger('main').info('starting...')
    app.run(host='0.0.0.0')
