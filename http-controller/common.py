import os
import re
from elasticsearch import Elasticsearch
import config


class DetonatorUpload:
    def __init__(self, upload_dir, sample, uuid, run_ids):
        self.upload_dir = upload_dir
        self.sample = sample
        self.uuid = uuid
        self.run_ids = run_ids

    def get_path(self, fn='', run_id=0):
        return os.path.join(self.upload_dir, self.sample, self.uuid, str(run_id), fn)

    def get_metadata(self, run_id=0):
        return self.get_path('run-{}-meta.json'.format(run_id), run_id)

    def get_output(self, run_id=0):
        return self.get_path('aext-mem-rw-dump.out', run_id)

    def __str__(self):
        return 'sample: {}, uuid: {}, run_ids: {}'.format(self.sample, self.uuid, self.run_ids)


def get_detonator_uploads(upload_dir):
    ret = []

    for sample in get_samples(upload_dir):
        for detonation_uuid in os.listdir(os.path.join(upload_dir, sample)):
            run_ids = os.listdir(os.path.join(upload_dir, sample, detonation_uuid))
            ret.append(DetonatorUpload(upload_dir, sample, detonation_uuid, run_ids))

    return ret


def get_samples(samples_dir):
    return [f for f in os.listdir(samples_dir) if re.match(r'^[A-Za-z0-9]{64}$', f, re.MULTILINE)]


def push_upload_stats_elastic(json_dir=config.UPLOADS_DIR, _index='malml', _doc_type='upload_metadata'):
    es = Elasticsearch()

    uploads = get_detonator_uploads(json_dir)
    for u in uploads:
        for r in u.run_ids:
            with open(u.get_metadata(r), 'r') as fd:
                _id = '{}-{}-{}'.format(u.sample, u.uuid, r)
                es.index(index=_index, doc_type=_doc_type, body=fd.read(), id=_id)
                print 'wrote {}'.format(_id)

if __name__ == '__main__':
    push_upload_stats_elastic()

