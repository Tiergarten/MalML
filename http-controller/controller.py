import json
import uuid
from flask import Flask

app = Flask(__name__)

#TODO: How should we store shared state?
uuid_run_map = {}

@app.route("/consumer/get_extract/<sample>/<uuid>/<extract>")
def get_extract(sample, uuid, extract):
	return "OK"

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

	
@app.route('/agent/callback/', defaults={'cb_uuid':None})
@app.route('/agent/callback/<cb_uuid>')
def callback(cb_uuid):
	first_run = False
	cb_uuid, resp = update_callback_details(cb_uuid) 

	if uuid_run_map[cb_uuid] == 0:
		resp['action'] = 'dl_exec_extractor_pack'
		resp['pack_url'] = 'http://localhost:5000/extractor_packs/asdf.zip' 
		return json.dumps(resp)

	resp['action'] = 'seek_and_destroy'
	return json.dumps(resp) 


@app.route('/agent/upload/<cb_uuid>/<run_id>')
def upload_results(cb_uuid, run_id, file_upload):
	pass


if __name__ == '__main__':
	app.run()
