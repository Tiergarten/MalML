
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

CALLBACK_URI = "http://localhost:8080/detonator/callback"
INSTALL_DIR = "C:\\detonator-agent\\"

def get_uuid(install_path):
	if not os.path.isfile(install_path+"uuid.txt"):
		return None

	return open(fpath).read()

def get_nodename():
	return platform.node()

def download_pack(pack_uri):
	pass

def save_uuid(uuid_str):
	pass

def run_pack_dropper():
	pass

def run_pack_pids():
	pass

def parse_callback_resp(callback_resp):
	jdata = json.loads(callback_resp)
	download_pack(jdata['extractor-pack'])

	if 'uuid' in jdata:
		save_uuid(jdata['uuid'])
		run_pack_dropper()
	else:
		run_pack_pids()
	
def do_callback(callback_uri):
	uuid = get_uuid(INSTALL_DIR)
	
	data = {}
	data['node'] = get_nodename()
	if uuid:
		data['uuid'] = uuid

	encoded_data = urllib.urlencode(data)
	req = urllib2.Request(CALLBACK_URI, encoded_data)
	parse_callback_resp(urllib2.open(req).read())


