import os
import re
from elasticsearch import Elasticsearch
import config
import json
import hashlib


class DetonationUpload:
    def __init__(self, upload_dir, sample, uuid, run_ids):
        self.upload_dir = upload_dir
        self.sample = sample
        self.uuid = uuid
        self.run_ids = run_ids

    def get_path(self, fn='', run_id=0):
        return os.path.join(self.upload_dir, self.sample, self.uuid, str(run_id), fn)

    def get_metadata_path(self, run_id=0):
        return self.get_path('run-{}-meta.json'.format(run_id), run_id)

    def get_metadata(self, run_id=0):
        with open(self.get_metadata_path(run_id), 'r') as fd:
            return json.loads(fd.read())

    def isSuccess(self, run_id=0):
        md = self.get_metadata(run_id)
        return 'status' in md.keys() and md['status'] != "ERR"

    def get_output(self, run_id=0):
        return self.get_path('aext-mem-rw-dump.out.gz', run_id)

    def __str__(self):
        return 'sample: {}, uuid: {}, run_ids: {}'.format(self.sample, self.uuid, self.run_ids)


class DetonationMetadata:
    def __init__(self, detonation):
        self. detonation = detonation

    def get_node(self):
        pass

    def get_extractor(self):
        return 'pack-1'


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
    try:
        os.makedirs(path)
    except:
        pass


def push_upload_stats_elastic(json_dir=config.UPLOADS_DIR, _index=config.REDIS_CONF_UPLOADS[0],
                              _doc_type=config.REDIS_CONF_UPLOADS[1]):
    es = get_elastic()

    uploads = get_detonator_uploads(json_dir)
    for u in uploads:
        for r in u.run_ids:
            j = u.get_metadata(r)
            _id = '{}-{}-{}'.format(u.sample, u.uuid, r)
            es.index(index=_index, doc_type=_doc_type, body=json.dumps(j), id=_id)
            print 'wrote {}'.format(_id)

if __name__ == '__main__':
    push_upload_stats_elastic()

