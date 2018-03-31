import importlib

import time
from common import *
import config

from feature_extractors import fext_common

FEXT_MAP = {'pack-1': [('fext_mem_rw_dump', 'FextMemRwDump')]}


def get_fexts_for_pack(pack_nm):
    return FEXT_MAP[pack_nm]


def get_feature_extractor_for_pack(module, clazz):
    return getattr(importlib.import_module('feature_extractors.{}'.format(module)), clazz)


def extract_features(du, run_id):
    metadata = DetonationMetadata(du)

    for (module, clazz) in get_fexts_for_pack(metadata.get_extractor()):
        feature_ext_class = get_feature_extractor_for_pack(module, clazz)

        feature_writer = fext_common.FeatureSetsWriter(config.FEATURES_DIR, upload.sample, run,
                                                       feature_ext_class.extractor_name,
                                                       feature_ext_class.extractor_ver)

        feature_ext_class(feature_writer).run(du.get_output(run))


class FeatureExtractorWorker(threading.Thread):
    def __init__(self, producer_queue_nm, worker_nm):
        threading.Thread.__init__(self)
        self.queue = ReliableQueue(producer_queue_nm, config.REDIS_FEATURE_WORKER_PREFIX, worker_nm)
        self.setName(worker_nm)
        self.logger = logging.getLogger('{}-{}'.format(self.__class__.__name__, worker_nm))
        self.running = True

    def _run(self):
        while self.running:
            self.logger.info('polling {} ...'.format(self.queue.get_processing_list()))
            msg = self.queue.dequeue()
            if msg is None:
                continue

            du = DetonationUpload.from_json(msg)
            extract_features(du, msg['run_id'])
            self.queue.commit(msg)

    def run(self):
        while self.running:
            try:
                self._run()
            except Exception as e:
                self.logger.error(e)


if __name__ == '__main__':

    setup_logging('feature_extractor')

    workers = [FeatureExtractorWorker(config.UPLOAD_RQUEUE_NAME, w) for w in range(0, 4)]

    for w in workers:
        w.start()

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logging.getLogger('main').info('Cleaning up threads...')
        for w in workers:
            w.running = False
