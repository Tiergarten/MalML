

import os

DETONATOR_DIR = os.path.dirname(os.path.realpath(__file__))

EXT_IF = 'http://192.168.1.145:5000'
EXTRACTOR_PACK_URL = '{}/agent/extractor_pack/default'.format(EXT_IF)

_ACTIVE_VMS = [('win7_sp1_ent-dec_2011', 'autorun v0.3', 7)] # TODO: map this to below
ACTIVE_VMS = []
for suite in _ACTIVE_VMS:
    for i in range(1, suite[2]+1):
        ACTIVE_VMS.append(('{}_vm{}'.format(suite[0], i), suite[1]))

AGENT_DIR = os.path.join(DETONATOR_DIR, 'guest-agent')
EXTRACTOR_PACK_DIR = os.path.join(DETONATOR_DIR, 'extractor-packs')

DATA_DIR = os.path.join(DETONATOR_DIR, 'data')

SAMPLES_DIR = os.path.join(DATA_DIR, 'samples')
UPLOADS_DIR = os.path.join(DATA_DIR, 'uploads')
FEATURES_DIR = os.path.join(DATA_DIR, 'features')
LOGS_DIR = os.path.join(DATA_DIR, 'logs')


ES_CONF_SAMPLES = ('malml-sample', 'metadata')
ES_CONF_UPLOADS = ('malml-upload', 'metadata')

VM_HEARTBEAT_TIMEOUT_MINS = 8


REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_UPLOAD_QUEUE_NAME = 'detonator-uploads'
REDIS_FEATURE_WORKER_PREFIX = 'du-worker'