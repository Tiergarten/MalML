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
import collections

__version__ = '0.0.1'

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

    def get_json_metadata(self, sample_nm, source, label):
        return {
                'malml-sample-mgr': __version__,
                'vti': self.get_vti_sample_metadata(sample_nm),
                'source': source,
                'label': label,
                'arch': self.get_arch(os.path.join(self.input_sample_dir, sample_nm))
            }

    def write_sample_metadata(self, sample_nm, source, label, elastic):
        sample_md_path = self.get_tgt_sample_metadata_file(sample_nm)
        gen_metadata = self.get_json_metadata(sample_nm, source, label)

        if not os.path.exists(sample_md_path):
            with open(sample_md_path, 'w') as fd:
                fd.write(json.dumps(gen_metadata, indent=4, sort_keys=True))
            print 'wrote {}'.format(sample_md_path)
        else:
            print '{} already exists, checking if ours'.format(sample_md_path)
            with open(sample_md_path, 'r') as fd:
                metadata = json.loads(fd.read())

            if 'malml-sample-mgr' not in metadata:
                print 'not ours, overwriting...'
                gen_metadata['existing_metadata'] = metadata
                with open(sample_md_path, 'r') as fd:
                    fd.write(json.dumps(metadata, indent=4, sort_keys=True))

        if elastic:
            try :
                self.write_sample_metadata_to_elastic(sample_nm, metadata)
            except Exception as e:
                print 'WARN: {}'.format(e)


    @staticmethod
    def pre_process_json_for_elastic(metadata):
        ret = metadata

        # elastic doesn't like a list having 2 different types...
        if 'additional_info' in ret['vti']:
            if 'sections' in ret['vti']['additional_info']:
                new_sections = []
                for s in metadata['vti']['additional_info']['sections']:
                    new_sections.append([str(c) for c in s])
                ret['vti']['additional_info']['sections'] = new_sections

            if 'rombioscheck' in ret['vti']['additional_info']:
                if 'manufacturer_candidates' in ret['vti']['additional_info']['rombioscheck']:
                    new_candidates = []
                    for c in ret['vti']['additional_info']['rombioscheck']['manufacturer_candidates']:
                        new_candidates.append([str(x) for x in c])
                    ret['vti']['additional_info']['rombioscheck']['manufacturer_candidates'] = new_candidates

        # elastic doesn't cope well with lots of unique field names...
        SampleImporter.cut(ret, 3, 'REDACTED')
        return ret

    @staticmethod
    def cut(dict_, maxdepth, replaced_with=None):
        """Cuts the dictionary at the specified depth.

        If maxdepth is n, then only n levels of keys are kept.
        """
        queue = collections.deque([(dict_, 0)])

        # invariant: every entry in the queue is a dictionary
        while queue:
            parent, depth = queue.popleft()
            for key, child in parent.items():
                if isinstance(child, dict):
                    if depth == maxdepth - 1:
                        parent[key] = replaced_with
                    else:
                        queue.append((child, depth + 1))

    def write_sample_metadata_to_elastic(self, sample_nm, _metadata_dict):
        metadata_dict = SampleImporter.pre_process_json_for_elastic(_metadata_dict)
        es = get_elastic()
        es.index(index=config.REDIS_CONF_SAMPLES[0], doc_type=config.REDIS_CONF_SAMPLES[1],
                 body=json.dumps(metadata_dict), id=sample_nm)

        print 'wrote {} -> elastic'.format(sample_nm)

    def sync_master_with_elastic(self):
        files = [f for f in os.listdir(self.master_sample_dir)
                 if re.match(r'^[A-Za-z0-9]{64}.json$', f, re.MULTILINE)]

        for f in files:
            sample_md_path = os.path.join(self.master_sample_dir, f)
            with open(sample_md_path, 'r') as fd:
                metadata = json.loads(fd.read())
                self.write_sample_metadata_to_elastic(f.replace('.json', ''), metadata)

    def copy_input_sample_to_master(self, sample):
        shutil.copyfile(os.path.join(self.input_sample_dir, sample),
                        os.path.join(self.master_sample_dir, sample))

    def import_samples(self, sample_src, sample_label, elastic=False):
        # Rename to sha256
        for f in os.listdir(self.input_sample_dir):
            if not is_sha256_fn(f) and not f.endswith('.txt'):
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

        # TODO: Look @ elastic to see what we've already processed...
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

    source = ''
    label = ''
    input_dir = ''
    existing_only = False

    opts, remaining = getopt.getopt(sys.argv[1:], 'qs:l:i:e',
                                    ['queue-samples', 'source', 'label', 'input-dir', 'existing'])

    for opt, arg in opts:
        if opt in ('-s', '--source'):
            source = arg
        if opt in ('-l', '--label'):
            label = arg
        if opt in ('-i', '--input-dir'):
            input_dir = arg
        if opt in ('-e', '--existing'):
            existing_only = True

    if existing_only:
        si = SampleImporter('', config.SAMPLES_DIR)
        si.sync_master_with_elastic()
    else:
        assert len(source) > 1 and len(input_dir) > 1 and len(label) > 1

        print 'importing samples {} -> {}'.format(input_dir, config.SAMPLES_DIR)
        print 'source: {}, label: {}'.format(source, label)

        si = SampleImporter(input_dir, config.SAMPLES_DIR)
        si.import_samples(source, label, elastic=True)
