
# check for uuid.txt, if present add it to callback
# callback to http-controller, download latest 'extractor-pack'
# 
# if this is first run, save uuid -> uuid.txt
# PinTool trace dropper.exe

# if this is > 1st run. scan for PID's that are not in whitelist
# Run each extractor for this PID(s)

# if this is last run, gzip extractor output & send back to controller

import os.path
import platform
import urllib2
import urllib
import json
import zipfile

CALLBACK_URI = "http://localhost:5000/agent/callback"

if os.name == 'nt':
    INSTALL_DIR = "C:\\detonator-agent"
else:
    INSTALL_DIR = '.'


def read_file(file):
    fd = open(file)
    ret = fd.read()
    fd.close()
    return ret


class MyURLopener(urllib.FancyURLopener):
  def http_error_default(self, url, fp, errcode, errmsg, headers):
      raise "Unable to download %s !" % url


class ExtractorPackManager:
    def __init__(self, pack_uri):
        self.pack_uri = pack_uri
        self.local_path = ""

    def download_pack(self):
        local_fn = self.pack_uri.split('/')[-1]
        local_fn_w_ext = local_fn + '.zip'
        self.local_path = 'extracted-' + local_fn

        m = MyURLopener()
        m.retrieve(self.pack_uri, local_fn_w_ext)

        zip_ref = zipfile.ZipFile(local_fn_w_ext, 'r')
        zip_ref.extractall(self.local_path)
        zip_ref.close()

        print "extracted %s -> %s" % (self.pack_uri, self.local_path)
        return self.local_path

    def get_extractors(self):
        return [e for e in os.listdir(self.local_path) if e.startswith('pack') and
                os.path.isdir(os.path.join(self.local_path, e))]

    def run_pack(self, mode='init'):
        sample = self.get_manifest(self.local_path)['sample']
        extractors = self.get_extractors()

        print 'sample: %s, extractors: %s' % (sample, extractors)

        for e in extractors:
            exec_str_key = 'run-'+mode
            exec_str = self.get_manifest(os.path.join(self.local_path, e))[exec_str_key]
            exec_str_pp = self.replace_placeholders(exec_str, {'SAMPLE': sample,
                                                        'NON_WHITELIST_PID' : '1024'})

            print exec_str_pp

    def get_manifest(self, path):
        data = read_file(os.path.join(path, 'manifest'))
        resp = {}
        for line in data.split('\n'):
            if not line:
                continue
            kv = line.split('=')
            resp[kv[0]] = kv[1]

        return resp

    def replace_placeholders(self, template_str, find_repl_dict):
        for k, v in find_repl_dict.iteritems():
            template_str = template_str.replace('<'+k+'>', v)

        return template_str


class EnvManager(object):
    @staticmethod
    def get_uuid_file_path():
        return os.path.join(INSTALL_DIR, "uuid.txt")

    @staticmethod
    def get_uuid():
        if not os.path.isfile(EnvManager.get_uuid_file_path()):
            return None

        return read_file(EnvManager.get_uuid_file_path())

    @staticmethod
    def save_uuid(uuid_str):
        fd = open(EnvManager.get_uuid_file_path(), 'w')
        fd.write(uuid_str)
        fd.close()

        print 'wrote uuid file'

    @staticmethod
    def get_nodename():
        return platform.node()


class AgentCallback:
    def __init__(self, callback_uri):
        self.callback_uri = callback_uri

    def parse_callback_resp(self, callback_resp):
        jdata = json.loads(callback_resp)
        print jdata

        if jdata['run_id'] == 0:
            EnvManager.save_uuid(jdata['uuid'])

        mgr = ExtractorPackManager(jdata['pack_url'])
        mgr.download_pack()
        mgr.run_pack(jdata['action'])

    def do_callback(self):
        uuid = EnvManager.get_uuid()

        data = {'node': EnvManager.get_nodename()}
        if uuid:
            data['uuid'] = uuid

        encoded_data = urllib.urlencode(data)
        req = urllib2.Request(self.callback_uri, encoded_data)
        self.parse_callback_resp(urllib2.urlopen(req).read())

    def run(self):
        self.do_callback()

if __name__ == '__main__':
    print "HELO from agent.py"
    cb = AgentCallback(CALLBACK_URI)
    cb.run()
