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

from common import *

http_headers = {
        "Accept-Encoding": "gzip, deflate",
        "User-Agent": "gzip,  My Python requests library example client or username"
}


class MetaDataWriter:
    def __init__(self, sample_dir):
        self.sample_dir = sample_dir

    def get_sample_metadata_file(self, sample):
        return os.path.join(config.SAMPLES_DIR, '{}.json'.format(sample))

    def get_samples_without_metadata(self, samples):
        return filter(lambda s: not os.path.isfile(self.get_sample_metadata_file(s)), get_samples(samples))

    def get_vti_sample_metadata(self, sha256):
        params = {'apikey': secrets.VT_API_KEY, 'resource': sha256, 'allinfo': 1}
        return requests.get('https://www.virustotal.com/vtapi/v2/file/report',
                            params=params, headers=http_headers).json()

    def write_sample_metadata_to_disk(self):
        cnt = 0
        for sample in self.get_samples_without_metadata(self.sample_dir):
            md = self.get_vti_sample_metadata(sample)

            fn = self.get_sample_metadata_file(sample)
            with open(fn, 'w') as fd:
                fd.write(json.dumps(md, indent=4, sort_keys=True))

            print 'wrote {}'.format(fn)
            cnt += 1

        print 'wrote json for {} samples'.format(cnt)

    def write_smaple_metadata_to_elastic(self):
        pass


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
            self.r.lpush(json.dumps({
                'sample_name': sample,
                'vm': 'win7_sp1_ent-dec_2011_vm1',
                'pack': 'pack-1'
            }))

    def run(self):
        while True:
            self.enqueue_samples()
            time.sleep(self.sleep_tm)

    def get_samples_without_upload(samples, uploads, node=''):
        if node == '':
            return filter(lambda s: not os.path.isdir(os.path.join(uploads, s)), get_samples(samples))


if __name__ == '__main__':
    opts, remaining = getopt.getopt(sys.argv[1:], 'mq', ['metadata, queue-samples'])
    for opt, arg in opts:
        if opt in ('-m', '--metadata'):
            print 'syncing metadata...'
            MetaDataWriter(config.SAMPLES_DIR).write_sample_metadata_to_disk()
        if opt in ('-q', '--queue-samples'):
            SampleEnqueuer((config.REDIS_HOST, config.REDIS_PORT, config.REDIS_QUEUE), 10).run()


