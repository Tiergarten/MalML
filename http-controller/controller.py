import json
import uuid
import pprint
import tempfile
import os

from flask import Flask, request, send_from_directory

app = Flask(__name__)

# TODO: How should we store shared state?
uuid_run_map = {}

AGENT_INSTALL_PATH = '../guest-agent/agent.py'
EXTRACTOR_PACK_PATH = "epack.zip"

EXT_IF = 'http://192.168.1.130:5000'
EXTRACTOR_PACK_URL = '{}/agent/extractor_pack/default'.format(EXT_IF)
SAMPLE_URL = '{}/agent/get_sample/71fc87528a591c3c6679e7d72b2c93b683fcc996f26769f0e0ee4264e1c5089a'.format(EXT_IF)

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
    resp['sample_url'] = SAMPLE_URL

    if uuid_run_map[cb_uuid] == 0:
        resp['action'] = 'init'

    resp['action'] = 'seek_and_destroy'

    # TODO: Remove this to return to normal flow
    resp['action'] = 'init'
    return json.dumps(resp)


@app.route('/agent/extractor_pack/<pack_name>')
def extractor_pack(pack_name):
    return send_from_directory("./extractor-packs/", "pack.zip")


@app.route('/agent/upload/<sample>/<uuid>/<run_id>', methods=['GET', 'POST'])
def upload_results(sample, uuid, run_id):
    upload_dir = os.path.join('uploads', sample, uuid, run_id)

    try:
        os.makedirs(upload_dir)
    except:
        pass

    for f in request.files:
        local_f = os.path.join(upload_dir, f)
        request.files[f].save(local_f)
        print 'wrote {}'.format(local_f)

    metadata_f = 'run-{}-meta.json'.format(run_id)
    with open(os.path.join(upload_dir, metadata_f), 'w') as metadata:
        metadata.write(json.dumps(request.form))
        print 'wrote {}'.format(metadata_f)

    return "OK"


@app.route('/agent/get_sample/<sha256>')
def get_sample(sha256):
    return send_from_directory("./samples/", "615cc5670435e88acb614c467d6dc9b09637f917f02f3b14cd8460d1ac6058ec")


if __name__ == '__main__':
    app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024
    app.run(debug=True, host='0.0.0.0')
