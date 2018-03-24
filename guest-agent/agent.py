
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
import requests
import subprocess
import datetime
import gzip
from threading import Timer
import time

# TODO: read this from ENV[]
CONTROLLER = "http://192.168.1.145:5000"
CALLBACK_URI = "{}/agent/callback".format(CONTROLLER)
UPLOAD_URI = "{}/agent/upload".format(CONTROLLER)
INSTALL_DIR = '.'
EXEC_TIMEOUT_MINS = 3


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
        self.whitelist = json.loads(EnvManager.read_file(f))

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
    def __init__(self, meta_data):
        self.pack_uri = meta_data['pack_url']
        self.sample_uri = meta_data['sample_url']

        self.sample = ''
        self.pack_path = ''

        self.meta_data = meta_data

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

    def get_non_whitelisted_pid(self):
        pw = ProcessWhitelist()
        non_whitelist_pid = pw.find_not_whitelisted_procs()

        if len(non_whitelist_pid) > 1:
            print 'WARN: Multiple new processes found! %s'.format(non_whitelist_pid)
        elif len(non_whitelist_pid) == 0:
            print 'WARN: No non whitelisted proc found'
            non_whitelist_pid = [0]

        return non_whitelist_pid

    def get_extractors(self):
        return [e for e in os.listdir(self.pack_path) if e.startswith('pack') and
                os.path.isdir(os.path.join(self.pack_path, e))]

    def run_pack(self, mode='init'):
        extractors = self.get_extractors()
        sample = self.sample
        print 'sample: {}, extractors: {}'.format(sample, extractors)

        if mode == 'seek_and_destroy':
            non_whitelist_pid = self.get_non_whitelisted_pid()
        else:
            non_whitelist_pid = 0

        for e in extractors:
            if e not in self.meta_data['extractor-pack'].split(','):
                continue

            extractor_dir = os.path.join(self.pack_path, e)
            exec_str_key = 'run-'+mode

            manifest = self.get_manifest(extractor_dir)

            if manifest is None:
                continue

            exec_str = manifest[exec_str_key]
            exec_str_pp = self.replace_placeholders(exec_str, {'SAMPLE': sample,
                                                               'NON_WHITELIST_PID': non_whitelist_pid})

            self.do_exec(extractor_dir, exec_str_pp)
            self.upload_output(manifest)

    def get_upload_path(self):
        return UPLOAD_URI+'/{}/{}/{}'.format(self.sample, self.meta_data['uuid'], self.meta_data['run_id'])

    def set_run_status(self, json, status, msg):
        json['status'] = status
        json['status_msg'] = msg

    # TODO: This needs re-written... too big
    def upload_output(self, manifest):

        fn = manifest['upload-file']
        self.meta_data['runs-left'] = int(manifest['total-runs']) - int(self.meta_data['run_id']) - 1
        self.meta_data['manifest'] = json.dumps(manifest)

        if os.path.isfile(fn):
            self.meta_data['output-before-gz'] = os.path.getsize(fn)
            print '{} gzipping {} ({})'.format(datetime.datetime.now(), fn, os.path.getsize(fn))
            gzipped_f = '{}.gz'.format(fn)

            with open(fn) as fd:
                with gzip.open(gzipped_f, 'wb') as gz:
                    gz.writelines(fd)

            self.meta_data['output-after-gz'] = os.path.getsize(gzipped_f)

            print '{} uploading {} ({})'.format(datetime.datetime.now(), gzipped_f, os.path.getsize(gzipped_f))

            with open(gzipped_f, 'rb') as gz:
                self.set_run_status(self.meta_data, 'OK', 'uploading at {}'.format(datetime.datetime.now()))
                r = requests.post(self.get_upload_path(), files={gzipped_f: gz}, data=self.meta_data)
                print r.text

            print 'upload finished: {}'.format(datetime.datetime.now())
        else:
            self.set_run_status(self.meta_data, 'ERR', 'no_file_to_upload')
            r = requests.post(self.get_upload_path(), data=self.meta_data)
            print r.text

    def do_exec(self, rundir, exec_str):
        execve_str = exec_str.split(' ')
        os.chdir(rundir)
        fqpn = os.path.abspath(execve_str[0])

        print 'calling: {} with args: {} from cwd: {}'.format(fqpn, execve_str[1:], os.getcwd())

        if os.path.isfile(fqpn):
            start_tm = datetime.datetime.now()
            print 'before pintool start: {}'.format(start_tm)
            self.meta_data['exec_start_tm'] = start_tm

            p = subprocess.Popen([fqpn]+execve_str[1:])
            psid = psutil.Process(p.pid)
            try:
                psid.wait(timeout=EXEC_TIMEOUT_MINS*60)
            except psutil.TimeoutExpired:
                self.kill_long_running_process(p.pid)

            end_tm = datetime.datetime.now()
            print 'after pintool: {}'.format(end_tm)
            self.meta_data['exec_end_tm'] = end_tm
        else:
            print 'ERR: %s not found' % fqpn

    def kill_long_running_process(self, pid):
        print 'timeout_mins breached, killing process'
        parent = psutil.Process(pid)
        for child in parent.children(recursive=True):
            try:
                child.kill()
            except:
                pass
        try:
            parent.kill()
        except:
            pass
        self.meta_data['INFO'] = 'breached_timeout'

    def get_manifest(self, path):
        mfile = os.path.join(path, 'manifest')
        if not os.path.isfile(mfile):
            print 'ERR: %s not found' % mfile
            return None

        data = EnvManager.read_file(mfile)
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
    def read_file(fn):
        ret = None
        with open(fn) as fd:
            ret = fd.read()

        return ret

    @staticmethod
    def write_env_file(fn, contents):
        with open(os.path.join(INSTALL_DIR, fn), 'w') as fd:
            fd.write(contents)

    @staticmethod
    def read_env_file(fn):
        path = os.path.join(INSTALL_DIR, fn)
        if os.path.isfile(path):
            return EnvManager.read_file(fn)
        else:
            return None

    @staticmethod
    def get_uuid():
        return EnvManager.read_env_file('uuid.txt')

    @staticmethod
    def save_uuid(uuid_str):
        EnvManager.write_env_file('uuid.txt', uuid_str)
        print 'wrote uuid file'

    @staticmethod
    def save_sample(sample):
        EnvManager.write_env_file('sample.txt', sample)

    @staticmethod
    def get_sample():
        return EnvManager.read_env_file('sample.txt')

    @staticmethod
    def get_nodename():
        return platform.node()


class AgentCallback:
    def __init__(self, callback_uri):
        self.callback_uri = callback_uri

    def parse_callback_resp(self, callback_resp):
        jdata = json.loads(callback_resp)

        # I don't think the server should track run_id...
        if jdata['run_id'] == 0:
            EnvManager.save_uuid(jdata['uuid'])
            EnvManager.save_sample(jdata['sample_url'])
        else:
            jdata['sample_url'] = EnvManager.get_sample()

        jdata['node'] = EnvManager.get_nodename()

        mgr = ExtractorPackManager(jdata)
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
    cb = AgentCallback(CALLBACK_URI)

    try:
        cb.run()
        time.sleep(60*1)
    except Exception as e:
        print 'ERRR!!!! {}'.format(e)
        time.sleep(60*5)
