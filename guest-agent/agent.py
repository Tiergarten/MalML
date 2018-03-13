
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
import psutil

# TODO: read this from ENV[]
CALLBACK_URI = "http://192.168.1.130:5000/agent/callback"
INSTALL_DIR = '.'


def read_file(file):
    fd = open(file)
    ret = fd.read()
    fd.close()
    return ret


class MyURLopener(urllib.FancyURLopener):
  def http_error_default(self, url, fp, errcode, errmsg, headers):
      raise "Unable to download %s !" % url


class ProcessWhitelist:
    def __init__(self, whitelist=''):
        if whitelist == '':
            self.whitelist = self.get_proclist()
        else:
            self.whitelist = whitelist

    def to_json(self, p):
        return {    'cwd': p.cwd(),
                    'proc': p.name(),
                    'argline': ' '.join(p.cmdline()),
                    'parent': p.parent().name()
                }

    def from_file(self, f):
        self.whitelist = json.loads(read_file(f))

    def to_file(self, f):
        fd = open(f, 'w')
        fd.write(json.dumps(self.whitelist))
        fd.close()

    def get_proclist(self):
        ret = []
        for pid in psutil.pids():
            try:
                p = psutil.Process(pid)
                ret.append(self.to_json(p))
            except:
                pass

        return ret

    def find_not_whitelisted_procs(self):
        ret = []
        for pid in psutil.pids():
            try:
                j = self.to_json(psutil.Process(pid))
                if j in self.whitelist:
                    continue
                else:
                    print 'found new process - %s' % j
                    ret.append(pid)
            except:
                pass

        return ret


class ExtractorPackManager:
    def __init__(self, pack_uri, sample_uri):
        self.pack_uri = pack_uri
        self.sample_uri = sample_uri

        self.sample = ''
        self.pack_path = ''

    def download_file(self, uri, lname):
        m = MyURLopener()
        m.retrieve(uri, lname)

    def download_pack(self):
        local_fn = self.pack_uri.split('/')[-1]
        local_fn_w_ext = local_fn + '.zip'
        self.pack_path = 'extracted-' + local_fn

        self.download_file(self.pack_uri, local_fn_w_ext)

        with zipfile.ZipFile(local_fn_w_ext, 'r') as zip_ref:
            zip_ref.extractall(self.pack_path)

        print "extracted %s -> %s" % (self.pack_uri, self.pack_path)

        self.sample = self.sample_uri.split('/')[-1]
        if not self.sample.endswith('.exe'):
            self.sample += '.exe'

        self.download_file(self.sample_uri, self.sample)
        print 'downloaded {} -> {}'.format(self.sample_uri, self.sample)

    def get_extractors(self):
        return [e for e in os.listdir(self.pack_path) if e.startswith('pack') and
                os.path.isdir(os.path.join(self.pack_path, e))]

    def run_pack(self, mode='init'):
        extractors = self.get_extractors()
        sample = self.sample
        print 'sample: %s, extractors: %s' % (sample, extractors)

        pw = ProcessWhitelist()
        non_whitelist_pid = pw.find_not_whitelisted_procs()

        if len(non_whitelist_pid) > 1:
            print 'WARN: Multiple new processes found! %s' % non_whitelist_pid
        elif len(non_whitelist_pid) == 0:
            print 'WARN: No non whitelisted proc found'
            non_whitelist_pid = [0]

        for e in extractors:
            extractor_dir = os.path.join(self.pack_path, e)
            exec_str_key = 'run-'+mode

            manifest = self.get_manifest(extractor_dir)

            if manifest is None:
                continue

            exec_str = manifest[exec_str_key]
            exec_str_pp = self.replace_placeholders(exec_str, {'SAMPLE': sample,
                'NON_WHITELIST_PID' : non_whitelist_pid[0]})

            self.do_exec(extractor_dir, exec_str_pp)

    def do_exec(self, rundir, exec_str):
        execve_str = exec_str.split(' ')
        os.chdir(rundir)
        fqpn = os.path.abspath(execve_str[0])

        print 'calling: {} with args: {} from cwd: {}'.format(fqpn, execve_str[1:], os.getcwd())
        if os.path.isfile(fqpn):
            os.execve(fqpn, execve_str, os.environ)
        else:
            print 'ERR: %s not found' % fqpn

    def get_manifest(self, path):
        mfile = os.path.join(path, 'manifest')
        if not os.path.isfile(mfile):
            print 'ERR: %s not found' % mfile
            return None

        data = read_file(mfile)
        resp = {}
        for line in data.split('\n'):
            if not line:
                continue
            kv = line.split('=')
            resp[kv[0]] = kv[1]

        return resp

    def replace_placeholders(self, template_str, find_repl_dict):
        for k, v in find_repl_dict.iteritems():
            template_str = template_str.replace('<'+str(k)+'>', str(v))

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
        with open(EnvManager.get_uuid_file_path(), 'w') as fd:
            fd.write(uuid_str)

        print 'wrote uuid file'

    @staticmethod
    def get_nodename():
        return platform.node()


class AgentCallback:
    def __init__(self, callback_uri):
        self.callback_uri = callback_uri

    def parse_callback_resp(self, callback_resp):
        jdata = json.loads(callback_resp)

        if jdata['run_id'] == 0:
            EnvManager.save_uuid(jdata['uuid'])

        mgr = ExtractorPackManager(jdata['pack_url'], jdata['sample_url'])
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
