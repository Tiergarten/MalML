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

from model_gen.input import get_sample_set_from_disk
from model_gen.mg_common import  SampleLabelPredictor

__version__ = '0.0.1'


class SampleEnqueuer(threading.Thread):
    def __init__(self, redis_conf, sleep_tm=10):
        threading.Thread.__init__(self)
        self.queue = ReliableQueue(config.REDIS_SAMPLE_QUEUE_NAME)
        self.sleep_tm = sleep_tm
        self.in_flight = self.get_in_flight_es()
        self.queue_depth_limit = 50
        self.running = True

    def get_samples_from_disk(self):
        return [f for f in os.listdir(config.SAMPLES_DIR) if is_sha256_fn(f)]

    def prioritize_pick(self, unprocessed, existing_malware, existing_benign):

        if len(existing_malware) > len(existing_benign):
            wanted = SampleLabelPredictor.BENIGN
        else:
            wanted = SampleLabelPredictor.MALWARE

        good = []
        for s in unprocessed:
            ds = DetonationSample(s)
            if SampleLabelPredictor(ds).get_label() == wanted:
                good.append(s)

        if len(good) > 0:
            return wanted, random.sample(good, 1)

        logging.warn('Unable to get class we want... ({})'.format(wanted))

        ret = random.sample(unprocessed, 1)
        if wanted == SampleLabelPredictor.MALWARE:
            return SampleLabelPredictor.BENIGN, ret
        else:
            return SampleLabelPredictor.MALWARE, ret

    def get_samples_to_process(self, count=1):
        unprocessed = [sample for sample in self.get_samples_from_disk()
                       if sample not in self.in_flight]

        logging.info('identified {} unprocessed samples'.format(len(unprocessed)))
        if len(unprocessed) == 0:
            return None

        malware, benign = get_sample_set_from_disk()

        ret = []
        for i in range(0, count):
            clazz, sample = self.prioritize_pick(unprocessed, malware, benign)
            logging.info('priority picked {} / {}'.format(clazz, sample[0:8]))
            ret = ret + sample

        return ret

    def get_in_flight_es(self):
        ret = []

        # TODO: I thinkt his is fetching all the data, when all we need is the headers
        uploads = get_all_es(UploadSearch.s())
        for u in uploads:
            # TODO: This isn't looking at uuid/run_id
            ret.append(u.meta['id'].split('-')[0])

        for sample_q_json in ReliableQueue(config.REDIS_SAMPLE_QUEUE_NAME).queue_items():
            ret.append(json.loads(sample_q_json)['sample'])

        for vm, snapshot in config.ACTIVE_VMS:
            ret.append(ReliableQueue(config.REDIS_SAMPLE_QUEUE_NAME, config.REDIS_NODE_PREFIX, vm).
                       processing_items())

        return ret

    def queue_depth_limit_breached(self):
        return self.queue.queue_depth() >= self.queue_depth_limit

    def is_priority(self, sample):
        return 0

    def get_queue_payload(self, sample):
        return {
            'sample': sample,
            'vm': 'win7_sp1_ent-dec_2011',
            'extractor-pack': 'pack-1',
            'arch': DetonationSample(sample).get_arch()
        }

    def enqueue(self, sample):
        payload = self.get_queue_payload(sample)

        logging.info('enqueueing sample - {}'.format(payload['sample']))
        self.queue.enqueue(json.dumps(payload), self.is_priority(payload['sample']))
        self.in_flight.append(payload['sample'])

    def _run(self, cnt):
        if self.queue_depth_limit_breached():
            logging.info('Queue depth limit {} breached, sleeping...'.format(self.queue_depth_limit))
            return

        # Re-sync with ES & Queues
        if cnt != 0 and cnt % 50 == 0:
            logging.info('Re-triggering sync with ES and queues')
            self.in_flight = self.get_in_flight_es()

        to_process = self.get_samples_to_process(self.queue_depth_limit - self.queue.queue_depth())
        if to_process is not None:
            for s in to_process:
                self.enqueue(s)

    def run(self):
        run_cnt = 0
        while self.running:
            try:
                self._run(run_cnt)
                run_cnt += 1
            except Exception as e:
                logging.exception(e)

            time.sleep(self.sleep_tm)

    @staticmethod
    def get_samples_without_upload(samples, uploads, node=''):
        if node == '':
            return filter(lambda s: not os.path.isdir(os.path.join(uploads, s)),
                          get_samples(samples))


if __name__ == '__main__':
    setup_logging('sample_mgr.log')

    clear_input_queue = False
    clear_processing_queues = False

    opts, excess = getopt.getopt(sys.argv[1:], 'ip', ['clear-input', 'clear-processing'])
    for opt, arg in opts:
        if opt in ('-i', '--clear-input'):
            clear_input_queue = True
        elif opt in ('-p', '--clear-processing'):
            clear_processing_queues = True

    if clear_input_queue:
        logging.info('Clearing {}'.format(config.REDIS_UPLOAD_QUEUE_NAME))
        ReliableQueue(config.REDIS_SAMPLE_QUEUE_NAME).clear_queue()

    if clear_processing_queues:
        logging.info('Clearing processing queues for {}'.format(config.REDIS_NODE_PREFIX))
        ReliableQueue(config.REDIS_SAMPLE_QUEUE_NAME, config.REDIS_NODE_PREFIX)\
            .clear_processing_queues()

    se = SampleEnqueuer((config.REDIS_HOST, config.REDIS_PORT))
    se.start()

    try:
        while True:
            time.sleep(30)
    except KeyboardInterrupt:
        logging.info('Signalling SampleEnqueuer to exit')
        se.running = False