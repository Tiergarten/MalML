import json
import uuid

from flask import Flask, request, send_from_directory

app = Flask(__name__)

# TODO: How should we store shared state?
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
    if cb_uuid is None:
        cb_uuid = uuid.uuid4().hex
        uuid_run_map[cb_uuid] = 0
    elif cb_uuid in uuid_run_map:
        uuid_run_map[cb_uuid] += 1
    else:
        print 'WARN: received uuid we dont know about...'
        uuid_run_map[cb_uuid] = 0

    resp['run_id'] = uuid_run_map[cb_uuid]
    resp['uuid'] = cb_uuid

    return cb_uuid, resp


def get_form_param(param_name):
    if param_name in request.form:
        return request.form.get(param_name)
    else:
        return None


# TODO: Run_id & action needs to be done per extractor?!
@app.route('/agent/callback', methods=['POST'])
def callback():

    cb_uuid = get_form_param('uuid')
    node = get_form_param('node')

    cb_uuid, resp = update_callback_details(cb_uuid)

    print "in callback - uuid: %s node: %s" % (cb_uuid, node)
    resp['pack_url'] = EXTRACTOR_PACK_URL

    if uuid_run_map[cb_uuid] == 0:
        resp['action'] = 'init'
        return json.dumps(resp)

    resp['action'] = 'seek_and_destroy'
    return json.dumps(resp)


@app.route('/agent/extractor_pack/<pack_name>')
def extractor_pack(pack_name):
    return send_from_directory("./samples/", "sample_ext_pack.zip")


@app.route('/agent/upload/<cb_uuid>/<run_id>')
def upload_results(cb_uuid, run_id, file_upload):
    pass


if __name__ == '__main__':
    app.run(debug=True)
