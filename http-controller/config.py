AGENT_INSTALL_PATH = '../guest-agent/agent.py'

EXT_IF = 'http://192.168.1.145:5000'
EXTRACTOR_PACK_URL = '{}/agent/extractor_pack/default'.format(EXT_IF)

ACTIVE_VMS = [('win7_sp1_ent-dec_2011_vm1', 'autorun v0.2'),
              ('win7_sp1_ent-dec_2011_vm2', 'autorun v0.2'),
              ('win7_sp1_ent-dec_2011_vm3', 'autorun v0.2'),
              ('win7_sp1_ent-dec_2011_vm4', 'autorun v0.2')]

REDIS_HOST = 'localhost'
REDIS_PORT = 6379

SAMPLES_DIR = 'samples'
AGENT_DIR = '../guest-agent/'
UPLOADS_DIR = 'uploads'
EXTRACTOR_PACK_DIR = 'extractor-packs'