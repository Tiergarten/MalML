import os
import re
import requests
import json
import getopt
import sys
import redis
import time
import threading

import secrets
import config
import subprocess
import hashlib

import shutil

from common import *

http_headers = {
        "Accept-Encoding": "gzip, deflate",
        "User-Agent": "gzip,  My Python requests library example client or username"
}


class SampleImporter:
    def __init__(self, input_sample_dir, master_sample_dir):
        self.input_sample_dir = input_sample_dir
        self.master_sample_dir = master_sample_dir

    def get_tgt_sample_metadata_file(self, sample):
        return os.path.join(config.SAMPLES_DIR, '{}.json'.format(sample))

    def get_samples_without_metadata(self, samples):
        return filter(lambda s: not os.path.isfile(self.get_tgt_sample_metadata_file(s)), get_samples(samples))

    def get_vti_sample_metadata(self, sha256):
        params = {'apikey': secrets.VT_API_KEY, 'resource': sha256, 'allinfo': 1}
        return requests.get('https://www.virustotal.com/vtapi/v2/file/report',
                            params=params, headers=http_headers).json()

    # TODO: error checking
    def get_arch(self, binary):
        cmd = 'file {}'.format(binary)
        ps = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        return ps.communicate()[0].split(':')[1].rstrip()

    def write_sample_metadata(self, sample_nm, source, label, elastic):
        sample_md_path = self.get_tgt_sample_metadata_file(sample_nm)

        if not os.path.exists(sample_md_path):
            metadata = {
                'vti': self.get_vti_sample_metadata(sample_nm),
                'source': source,
                'label': label,
                'arch': self.get_arch(os.path.join(self.input_sample_dir, sample_nm))
            }

            with open(sample_md_path, 'w') as fd:
                fd.write(json.dumps(metadata, indent=4, sort_keys=True))

            print 'wrote {}'.format(sample_md_path)
        else:
            print '{} already exists'.format(sample_md_path)
            with open(sample_md_path, 'r') as fd:
                metadata = json.loads(fd.read())

        # elastic doesn't like a list having 2 different types...
        new_sections = []
        for s in metadata['vti']['additional_info']['sections']:
            new_sections.append([str(c) for c in s])
        metadata['vti']['additional_info']['sections'] = new_sections

        if elastic:
            self.write_sample_metadata_to_elastic(sample_nm, json.dumps(metadata))

    def write_sample_metadata_to_elastic(self, sample_nm, json_str):
        es = get_elastic()
        es.index(index=config.REDIS_CONF_SAMPLES[0], doc_type=config.REDIS_CONF_SAMPLES[1],
                 body=json_str, id=sample_nm)

        print 'wrote {} -> elastic'.format(sample_nm)

    def copy_input_sample_to_master(self, sample):
        shutil.copyfile(os.path.join(self.input_sample_dir, sample),
                        os.path.join(self.master_sample_dir, sample))

    def import_samples(self, sample_src, sample_label, elastic=False):
        # Rename to sha256
        for f in os.listdir(self.input_sample_dir):
            if not is_sha256_fn(f):
                sample = os.path.join(self.input_sample_dir, f)
                target = os.path.join(self.input_sample_dir, sha256_checksum(sample))
                shutil.move(sample, target)
                print '{} -> {}'.format(sample, target)

        for s in get_samples(self.input_sample_dir):
            self.write_sample_metadata(s, sample_src, sample_label, elastic)
            self.copy_input_sample_to_master(s)


class SampleEnqueuer(threading.Thread):
    def __init__(self, redis_conf, sleep_tm):
        self.r = redis.Redis(host=redis_conf[0], port=redis_conf[1])
        self.queue_name = redis_conf[1]
        self.sleep_tm = sleep_tm

    def enqueue_samples(self):
        to_process = self.get_samples_without_upload(
            get_samples(config.SAMPLES_DIR), config.UPLOADS_DIR)

        if len(to_process) == 0:
            return

        for sample in to_process:
            for vm in get_active_vms():
                for pack in get_active_packs(vm):
                    self.r.lpush(json.dumps({
                        'sample_name': sample,
                        'vm': vm,
                        'pack': pack
                    }))

    def run(self):
        while True:
            self.enqueue_samples()
            time.sleep(self.sleep_tm)

    @staticmethod
    def get_samples_without_upload(samples, uploads, node=''):
        if node == '':
            return filter(lambda s: not os.path.isdir(os.path.join(uploads, s)),
                          get_samples(samples))


if __name__ == '__main__':

    do_import = False
    source = ''
    label = ''
    input_dir = ''
    do_queue = False

    opts, remaining = getopt.getopt(sys.argv[1:], 'mqs:l:i:',
                                    ['metadata, queue-samples', 'source', 'label', 'input-dir'])

    for opt, arg in opts:
        if opt in ('-m', '--metadata'):
            do_import = True
        if opt in ('-s', '--source'):
            source = arg
        if opt in ('-l', '--label'):
            label = arg
        if opt in ('-i', '--input-dir'):
            input_dir = arg
        if opt in ('-q', '--queue-samples'):
            do_queue = True

    # TODO: sync option, to take all json from samples -> elastic!

    if do_import:
        assert len(source) > 1 and len(input_dir) > 1 and len(label) > 1

        print 'importing samples {} -> {}'.format(input_dir, config.SAMPLES_DIR)
        print 'source: {}, label: {}'.format(source, label)

        si = SampleImporter(input_dir, config.SAMPLES_DIR)
        si.import_samples(source, label, elastic=True)

    if do_queue:
        pass
