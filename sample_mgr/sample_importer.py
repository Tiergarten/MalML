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

import random

__version__ = '0.0.1'


class SampleMetadataManager:
    vti_http_headers = {
        "Accept-Encoding": "gzip, deflate",
        "User-Agent": "gzip,  My Python requests library example client or username"
    }

    def __init__(self, s):
        self.detonation_sample = DetonationSample(s)

    def get_fn(self):
        return self.detonation_sample.metadata_file()

    def vti_metadata(self, all_info=0):
        params = {'apikey': secrets.VT_API_KEY, 'resource': self.detonation_sample.sample}
        if all_info:
            params['allinfo'] = 1

        return requests.get('https://www.virustotal.com/vtapi/v2/file/report',
                            params=params, headers=SampleMetadataManager.vti_http_headers).json()

    def produce_metadata(self, arch, source, label, existing, vti):
        ret = {
            'malml-sample-mgr': __version__,
            'source': source,
            'arch': arch
        }

        if vti:
            ret['vti'] = self.vti_metadata()
        if label:
            ret['label'] = label
        if existing:
            ret['existing'] = json.loads(existing)

        return ret

    def write_metadata(self, arch, source, label, existing, vti):
        md_json = self.produce_metadata(arch, source, label, existing, vti)
        with open(self.get_fn(), 'w') as fd:
            fd.write(json.dumps(md_json, indent=4, sort_keys=True))

    def to_es(self):
        es = get_elastic()
        md = self.detonation_sample.get_metadata()

        # Don't overload ES with too much data..
        SampleMetadataManager.cut(md, 4, 'REDACTED')
        if 'existing' in md.keys():
            md['existing']['additional_info'] = 'REDACTED'
        if 'vti' in md.keys():
            md['vti']['additional_info'] = 'REDACTED'

        es.index(index=config.ES_CONF_SAMPLES[0], doc_type=config.ES_CONF_SAMPLES[1],
                 body=json.dumps(md), id=self.detonation_sample.sample)

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


class SampleImporter:
    def __init__(self, input_dir):
        self.input_dir = input_dir
        self.tgt_dir = config.SAMPLES_DIR

    def get_samples_without_metadata(self):
        return filter(lambda s: not os.path.isfile(SampleMetadataManager(s).get_fn()), get_samples(self.tgt_dir))

    def valid_sample(self, file_output):
        if not file_output.startswith('PE32'):
            return False

        if '(DLL)' in file_output:
            return False

        return True

    def get_tgt_path(self, src_path):
        sample_nm = sha256_checksum(src_path)
        return os.path.join(config.SAMPLES_DIR, sample_nm)

    def import_binary(self, src_path):
        tgt = self.get_tgt_path(src_path)
        shutil.copy(src_path, tgt)
        logging.info('imported {} -> {}'.format(src_path, tgt))

    # TODO: too many args
    def write_metadata(self, src_path, sample_nm, arch, source, label, elastic, vti):
        existing = None
        src_json = '{}.json'.format(src_path)
        if os.path.exists(src_json):
            with open(src_json, 'r') as fd:
                existing = fd.read()

        md = SampleMetadataManager(sample_nm)
        md.write_metadata(arch, source, label, existing, vti)

        if elastic:
            md.to_es()

        logging.info('wrote metadata for {}'.format(sample_nm))

    def import_samples(self, source, label=None, elastic=False, vti=True):
        for f in os.listdir(self.input_dir):
            source_f = os.path.join(self.input_dir, f)
            sample_nm = sha256_checksum(source_f)

            if os.path.exists(self.get_tgt_path(source_f)):
                logging.info('{} already exists, skipping...'.format(sample_nm))

            file_type = get_arch(source_f)

            if not self.valid_sample(file_type):
                continue

            self.import_binary(source_f)
            self.write_metadata(source_f, sample_nm, file_type, source, label, elastic, vti)

    def sync_master_with_elastic(self):
        files = [f for f in os.listdir(self.tgt_dir)
                 if re.match(r'^[A-Za-z0-9]{64}.json$', f, re.MULTILINE)]

        for f in files:
            mdmgr = SampleMetadataManager(f.replace('.json', ''))
            mdmgr.to_es()

        logging.info('Synced {} with elastic'.format(self.tgt_dir))


if __name__ == '__main__':

    source = ''
    label = None
    input_dir = ''
    existing_only = False

    setup_logging('sample_importer.log')

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
        si = SampleImporter('')
        si.sync_master_with_elastic()
    else:
        assert len(source) > 1 and len(input_dir) > 1, \
            "usage: ./asdf.py -s <source> -i <input_dir> -l <label>"

        logging.info('importing samples {} -> {}'.format(input_dir, config.SAMPLES_DIR))
        logging.info('source: {}, label: {}'.format(source, label))

        si = SampleImporter(input_dir)
        si.import_samples(source, label, elastic=True, vti=True)
