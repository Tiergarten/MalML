
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


def get_uuid_file_path():
    return os.path.join(INSTALL_DIR, "uuid.txt")


def get_uuid():
    if not os.path.isfile(get_uuid_file_path()):
        return None

    return open(get_uuid_file_path()).read()


def get_nodename():
    return platform.node()


class MyURLopener(urllib.FancyURLopener):
  def http_error_default(self, url, fp, errcode, errmsg, headers):
      raise "FECK"


def download_pack(pack_uri):

    local_fn = pack_uri.split('/')[-1]
    local_fn_w_ext = local_fn + '.zip'
    extracted_pack = 'extracted-' + local_fn

    m = MyURLopener()
    m.retrieve(pack_uri, local_fn_w_ext)

    zip_ref = zipfile.ZipFile(local_fn_w_ext, 'r')
    zip_ref.extractall(extracted_pack)
    zip_ref.close()

    print "extracted %s -> %s" % (pack_uri, extracted_pack)
    return extracted_pack


def save_uuid(uuid_str):
    fd = open(get_uuid_file_path(), 'w')
    fd.write(uuid_str)
    fd.close()

    print 'wrote uuid file'


def run_pack_dropper(pack_dir):
    print os.listdir(pack_dir)


def run_pack_pids(pack_dir):
    pass


def parse_callback_resp(callback_resp):
    jdata = json.loads(callback_resp)
    print jdata

    if jdata['run_id'] == 0:
        save_uuid(jdata['uuid'])

    if 'uuid' in jdata:
        pack_dir = download_pack(jdata['pack_url'])
        run_pack_dropper(pack_dir)
    else:
        run_pack_pids('extracted-'+jdata['pack_url'].split('/')[-1])


def do_callback(callback_uri):
    uuid = get_uuid()

    data = {'node': get_nodename()}
    if uuid:
        data['uuid'] = uuid

    encoded_data = urllib.urlencode(data)
    req = urllib2.Request(callback_uri, encoded_data)
    parse_callback_resp(urllib2.urlopen(req).read())

if __name__ == '__main__':
    print "HELO from agent.py"
    do_callback(CALLBACK_URI)
