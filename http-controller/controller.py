import json
import uuid

from flask import Flask, request, send_from_directory

app = Flask(__name__)

#TODO: How should we store shared state?
uuid_run_map = {}

AGENT_INSTALL_PATH = '../guest-agent/agent.py'
EXTRACTOR_PACK_PATH = "epack.zip"

EXTRACTOR_PACK_URL = 'http://localhost:5000/agent/extractor_pack/default'

#
# Consumer
#

@app.route("/consumer/get_extract/<sample>/<uuid>/<extract>")
def get_extract(sample, uuid, extract):
	return "OK"

#
# Agent Stub
#

@app.route('/agent-stub/get-agent')
def get_agent():
	return send_from_directory("../guest-agent/", "agent.py")

#
# Agent
#

def update_callback_details(cb_uuid):
	resp = {}
	if cb_uuid == None:
		cb_uuid = uuid.uuid4().hex
		uuid_run_map[cb_uuid] = 0
	else:
		uuid_run_map[cb_uuid] = uuid_run_map[cb_uuid] + 1

	resp['run_id'] = uuid_run_map[cb_uuid]
	resp['uuid'] = cb_uuid

	return cb_uuid, resp


# TODO: Run_id & action needs to be done per extractor?!
@app.route('/agent/callback', methods = ['GET', 'POST'])
def callback():
	print "in callback"
	first_run = False

	cb_uuid = request.args['uuid']
	node = request.args['node']

	cb_uuid, resp = update_callback_details(cb_uuid) 

	if uuid_run_map[cb_uuid] == 0:
		resp['action'] = 'init'
		resp['pack_url'] = EXTRACTOR_PACK_URL
		return json.dumps(resp)

	resp['action'] = 'seek_and_destroy'
	return json.dumps(resp) 

@app.route('/agent/extractor_pack/<pack_name>')
def extractor_pack(pack_name):
	return app.send_static_file(EXTRACTOR_PACK_PATH)


@app.route('/agent/upload/<cb_uuid>/<run_id>')
def upload_results(cb_uuid, run_id, file_upload):
	pass


if __name__ == '__main__':
	app.run(debug=True)
