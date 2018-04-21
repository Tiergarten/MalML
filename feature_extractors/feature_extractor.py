import importlib
import sys
import time
from common import *
import config
import traceback
import logging
import objgraph
import gc
import getopt

from feature_extractors import fext_common

FEXT_MAP = {'pack-1': [('fext_mem_rw_dump', 'FextMemRwDump')]}


def get_fexts_for_pack(pack_nm):
    return FEXT_MAP[pack_nm]


def get_feature_extractor_for_pack(module, clazz):
    return getattr(importlib.import_module('feature_extractors.{}'.format(module)), clazz)


def extract_features(du, run_id, id, force=False):
    metadata = DetonationMetadata(du)

    for (module, clazz) in get_fexts_for_pack(metadata.get_extractor_pack()):
        feature_ext_class = get_feature_extractor_for_pack(module, clazz)

        feature_writer = fext_common.FeatureSetsWriter(config.FEATURES_DIR, du.sample, run_id,
                                                       feature_ext_class.extractor_name,
                                                       feature_ext_class.__version__,
                                                       metadata)

        if feature_writer.already_exists() and not force:
            logging.info('feature extract for {} already exists, skipping...'.format(du.sample))
            continue

        feature_ext_class(feature_writer,id).run(du.get_output(run_id))

        # Doesn't seem to be garbage collected due to the dynamic way it's instantiated
        del feature_ext_class
        gc.collect()


class FeatureExtractorWorker(threading.Thread):
    def __init__(self, producer_queue_nm, worker_nm):
        threading.Thread.__init__(self)
        self.worker_nm = worker_nm
        self.queue = ReliableQueue(producer_queue_nm, config.REDIS_FEATURE_WORKER_PREFIX, worker_nm)
        self.setName(worker_nm)
        self.logger = logging.getLogger('{}-{}'.format(self.__class__.__name__, worker_nm))
        self.running = True

    def _run(self):
        if self.queue.processing_depth() > 0:
            logging.info('enter recovery mode')
            while self.queue.processing_depth > 0:
                msg = self.queue.dequeue_recovery()
                if msg is None:
                    break

                self.process(msg[1])
            logging.info('exit recovery mode')
        while self.running:
            self.logger.info('polling {}, queue depth: {}, processing depth: {}'.format(self.queue.get_processing_list_nm(),
                                                                                        self.queue.queue_depth(),
                                                                                        self.queue.processing_depth()))
            self.logger.info('mem growth: {}'.format(str(objgraph.growth(limit=10))))

            msg = self.queue.dequeue(lifo=True)
            if msg is None:
                continue
            self.process(msg)

    def process(self, msg):
        du = DetonationUpload.from_json(msg)
        if du.isSuccess():
            extract_features(du, int(json.loads(msg)['run_id']), self.worker_nm)
        else:
            self.logger.info('skipping {}, not successful'.format(du.sample))

        self.queue.commit(msg)

    def run(self):
        while self.running:
            try:
                self._run()
            except Exception as e:
                self.logger.error(e)
                self.logger.error(traceback.format_exc())


def fext_daemon(worker_name, worker_count):
    workers = [FeatureExtractorWorker(config.REDIS_UPLOAD_QUEUE_NAME,
                                      '{}-{}'.format(worker_name, w)) for w in range(0, worker_count)]

    for w in workers:
        w.start()

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logging.getLogger('main').info('Cleaning up threads...')
        for w in workers:
            w.running = False


if __name__ == '__main__':
    init_queue = False
    daemon = False
    force = False
    worker_name = 'default'
    worker_count = 2
    oneshot_sample = None

    setup_logging('feature_extractor.log')

    opts, ret = getopt.getopt(sys.argv[1:], 'dco:ft:n:', ['daemon', 'clean', 'one-shot=', 'force', 'threads', 'name'])
    for opt, arg in opts:
        if opt in ('-d', '--daemon'):
            daemon = True
        elif opt in ('-c', '--clean'):
            init_queue = True
        elif opt in ('-o', '--one-shot'):
            oneshot_sample = arg
        elif opt in ('-f', '--force'):
            force = True
        elif opt in ('-t', '--threads'):
            worker_count = int(arg)
        elif opt in ('-n', '--name'):
            worker_name = arg

    if init_queue:
        ReliableQueue(config.REDIS_UPLOAD_QUEUE_NAME).clear_processing_queues()
        ReliableQueue(config.REDIS_UPLOAD_QUEUE_NAME).clear_queue()

    if daemon:
        fext_daemon(worker_name, worker_count)
    elif oneshot_sample is not None:
        sample, uuid = oneshot_sample.split(':')
        du = DetonationUpload(config.UPLOADS_DIR, sample, uuid, [0])
        extract_features(du, 0, force)
