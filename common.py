import os
import re
from elasticsearch import Elasticsearch
import config
import json
import hashlib
import logging
from logging.handlers import TimedRotatingFileHandler
import os
import sys
import redis
import threading
from config import LOGS_DIR
import subprocess
import psutil


class DetonationSample:
    def __init__(self, sample):
        self.sample = sample

    def get_arch(self):
        arch = self.get_metadata()['arch']
        if 'PE32+' in arch:
            return '64'
        elif 'PE' in arch:
            return '32'
        else:
            return None

    def get_metadata(self):
        with open(os.path.join(config.SAMPLES_DIR, '{}.json'.format(self.sample)), 'r') as fd:
            return json.load(fd)

    def get_source(self):
        return self.get_metadata()['source']


# TODO: This should only contain one run_id, its crap having to store (du, run_id)
class DetonationUpload:
    def __init__(self, upload_dir, sample, uuid, run_ids):
        self.upload_dir = upload_dir
        self.sample = sample
        self.uuid = uuid
        self.run_ids = run_ids

        create_dirs_if_not_exist(os.path.dirname(self.get_path()))

    def get_path(self, fn='', run_id=0):
        return os.path.join(self.upload_dir, self.sample, self.uuid, str(run_id), fn)

    def get_metadata_path(self, run_id=0):
        return self.get_path('run-{}-meta.json'.format(run_id), run_id)

    def get_metadata(self, run_id=0):
        with open(self.get_metadata_path(run_id), 'r') as fd:
            return json.loads(fd.read())

    def md_exists(self, run_id=0):
        if not os.path.exists(self.get_metadata_path(run_id)):
            return False

        return True

    def isSuccess(self, run_id=0):
        if not self.md_exists(run_id):
            return False

        md = self.get_metadata(run_id)
        return 'status' in md.keys() and md['status'] != "ERR"

    def get_output(self, run_id=0):
        return self.get_path('aext-mem-rw-dump.out.gz', run_id)

    def __str__(self):
        return 'sample: {}, uuid: {}, run_ids: {}'.format(self.sample, self.uuid, self.run_ids)

    def write_metadata(self, md_body, run_id=0):
        with open(self.get_metadata_path(run_id), 'w') as md:
            md.write(md_body)

    def to_json(self, run_id=0):
        return json.dumps({
            'uuid': self.uuid,
            'sample': self.sample,
            'run_id': run_id
        })

    @staticmethod
    def from_json(json_body):
        j = json.loads(json_body)
        return DetonationUpload(config.UPLOADS_DIR, j['sample'], j['uuid'], [j['run_id']])


class DetonationMetadata:
    def __init__(self, detonation):
        self.detonation_upload = detonation
        self.metadata = self.detonation_upload.get_metadata()

    def get_node(self):
        return self.metadata['node']

    def get_extractor_pack(self):
        return self.metadata['extractor-pack']

    def get_uuid(self):
        return self.metadata['uuid']


def get_detonator_uploads(upload_dir):
    ret = []

    for sample in get_samples(upload_dir):
        for detonation_uuid in os.listdir(os.path.join(upload_dir, sample)):
            try:
                run_ids = os.listdir(os.path.join(upload_dir, sample, detonation_uuid))
                ret.append(DetonationUpload(upload_dir, sample, detonation_uuid, run_ids))
            except:
                print 'runs for {} in error state'.format(sample)
                continue

    return ret


def enqueue_existing_uploads_for_feature_ext():
    q = ReliableQueue(config.REDIS_UPLOAD_QUEUE_NAME)
    for upload in get_detonator_uploads(config.UPLOADS_DIR):
        if upload.isSuccess():
            print 'sending {}'.format(upload.to_json())
            q.enqueue(upload.to_json())


def get_samples(samples_dir):
    return [f for f in os.listdir(samples_dir) if is_sha256_fn(f)]


def is_sha256_fn(fn):
    return re.match(r'^[A-Za-z0-9]{64}$', fn, re.MULTILINE)


def get_elastic():
    return Elasticsearch()


def get_active_vms():
    return ['win7_sp1_ent-dec_2011_vm1']


def get_active_packs(vm):
    packs_per_vm = {'win7_sp1_ent-dec_2011_vm1': ['pack-1']}
    return packs_per_vm[vm]


def sha256_checksum(filename, block_size=65536):
    sha256 = hashlib.sha256()
    with open(filename, 'rb') as f:
        for block in iter(lambda: f.read(block_size), b''):
            sha256.update(block)
    return sha256.hexdigest()


def get_feature_families_produced_by_pack(pack_nm):
    if pack_nm == 'pack-1':
        return ['mem_rw_dump']


def create_dirs_if_not_exist(path):
    if os.path.isfile(path):
        _path = os.path.basename(path)
    else:
        _path = path

    try:
        os.makedirs(_path)
    except:
        pass


def set_run_status(json, status, msg):
    json['status'] = status
    json['status_msg'] = msg


def setup_logging(_log_fn):
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    log_fn = os.path.join(LOGS_DIR, _log_fn)

    file_log_handler = TimedRotatingFileHandler(log_fn, when='D')
    file_log_handler.setLevel(logging.INFO)
    file_log_handler.setFormatter(formatter)

    console_log_handler = logging.StreamHandler(sys.stdout)
    console_log_handler.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(file_log_handler)
    root.addHandler(console_log_handler)

    return file_log_handler, console_log_handler


def detonation_upload_to_es(detonation_upload, run_id):
    es = get_elastic()
    _id = '{}-{}-{}'.format(detonation_upload.sample, detonation_upload.uuid, run_id)

    if not detonation_upload.md_exists(run_id):
        print 'skipping {}'.format(detonation_upload.sample)
        return

    metadata = detonation_upload.get_metadata(run_id)
    metadata['sample'] = detonation_upload.sample

    es.index(index=config.ES_CONF_UPLOADS[0], doc_type=config.ES_CONF_UPLOADS[1],
             body=json.dumps(metadata), id=_id)
    print 'wrote -> elastic {}'.format(_id)


def push_upload_stats_elastic(json_dir=config.UPLOADS_DIR):
    uploads = get_detonator_uploads(json_dir)
    for u in uploads:
        for r in u.run_ids:
            detonation_upload_to_es(u, r)


def get_redis():
    return redis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT)


class ReliableQueue:
    def __init__(self, producer_queue, consumer_queue_prefix=None, consumer_id=None):
        self.producer_queue = producer_queue
        self.consumer_queue_prefix = consumer_queue_prefix
        self.consumer_id = consumer_id
        self.r = get_redis()
        self.blocking_timeout = 15

    def get_processing_list(self):
        return '{}:{}'.format(self.consumer_queue_prefix, self.consumer_id)

    def enqueue(self, msg):
        self.r.lpush(self.producer_queue, msg)

    def dequeue(self):
        return self.r.brpoplpush(self.producer_queue, self.get_processing_list(), self.blocking_timeout)

    def dequeue_recovery(self):
        return self.r.blpop(self.get_processing_list(), self.blocking_timeout)

    def commit(self, msg):
        return self.r.lrem(self.get_processing_list(), msg)

    def queue_depth(self):
        return self.r.llen(self.producer_queue)

    def processing_depth(self):
        return self.r.llen(self.get_processing_list())

    def clear_queue(self):
        self.r.delete(self.producer_queue)

    def clear_processing_queues(self):
        for k in self.r.keys('{}:*'.format(self.consumer_queue_prefix)):
            self.r.delete(k)


class TimeoutExec:
    def __init__(self, cmdline, timeout_mins):
        self.cmdline = cmdline
        self.timeout = timeout_mins

    def do_exec(self):
        logging.info('calling {}'.format(self.cmdline))

        p = subprocess.Popen(self.cmdline, shell=True)

        psid = psutil.Process(p.pid)
        try:
            psid.wait(timeout=1 * 60)
        except psutil.TimeoutExpired:
            self.kill_long_running_process(p.pid)

    def kill_long_running_process(self, pid):
        logging.error('Timeout breached - {}'.format(self.cmdline))
        parent = psutil.Process(pid)
        for child in parent.children(recursive=True):
            try:
                child.kill()
            except:
                pass
        try:
            parent.kill()
        except:
            pass
        logging.warn('killed long running script: {}'.format(pid))