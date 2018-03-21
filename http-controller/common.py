import os
import re
from elasticsearch import Elasticsearch
import config


def get_samples(samples_dir):
    return [f for f in os.listdir(samples_dir) if re.match(r'^[A-Za-z0-9]{64}$', f, re.MULTILINE)]


def push_json_into_elastic(json_dir):
    es = Elasticsearch()

    wrote_count = 0
    err_count = 0
    duplicates = 0

    # curl -XDELETE localhost:9200/malml/
    for sample in get_samples(config.UPLOADS_DIR):
        detonations_per_uuid = 0
        for detonation_uuid in os.listdir(os.path.join(config.UPLOADS_DIR, sample)):
            try:
                for run_id in os.listdir(os.path.join(config.UPLOADS_DIR, sample, detonation_uuid)):
                    fn = os.path.join(config.UPLOADS_DIR, sample, detonation_uuid, run_id, 'run-0-meta.json')
                    with open(fn, 'r') as fd:
                        es.index(index='malml', doc_type='upload_metadata', body=fd.read(), id=sample)
                        print 'wrote {}'.format(fn)
                        wrote_count += 1
                detonations_per_uuid += 1
            except:
                print 'bad entry {}'.format(sample)
                err_count +=1

            if detonations_per_uuid > 1:
                duplicates += (detonations_per_uuid)

    print 'good: {}, bad: {}, dups: {}, total uploaded: {}'.format(
        wrote_count, err_count, duplicates, wrote_count - duplicates)


if __name__ == '__main__':
    push_json_into_elastic(config.UPLOADS_DIR)
