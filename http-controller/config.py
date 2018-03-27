AGENT_INSTALL_PATH = '../guest-agent/agent.py'

EXT_IF = 'http://192.168.1.145:5000'
EXTRACTOR_PACK_URL = '{}/agent/extractor_pack/default'.format(EXT_IF)

_ACTIVE_VMS = [('win7_sp1_ent-dec_2011', 'autorun v0.3', 4)] # TODO: map this to below

ACTIVE_VMS = []
for suite in _ACTIVE_VMS:
    for i in range(1, suite[2]+1):
        ACTIVE_VMS.append(('{}_vm{}'.format(suite[0], i), suite[1]))

REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_QUEUE = 'sample_queue'

SAMPLES_DIR = 'samples'
AGENT_DIR = '../guest-agent/'
UPLOADS_DIR = 'uploads'
EXTRACTOR_PACK_DIR = 'extractor-packs'
FEATURES_DIR = 'features/'


REDIS_CONF_SAMPLES = ('malml-sample', 'metadata')
REDIS_CONF_UPLOADS = ('malml-upload', 'metadata')

VM_HEARTBEAT_TIMEOUT_MINS = 8