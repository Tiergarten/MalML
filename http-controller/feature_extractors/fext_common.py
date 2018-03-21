import json
import gzip

from json import encoder
encoder.FLOAT_REPR = lambda o: format(o, '.2f')

def pp_pin_output(output):
    ret = []
    for line in output:
        if line.startswith('#'):
            continue
        ret.append(line.rstrip())

    return ret


def get_pintool_output(fn):
    if fn.endswith('.gz'):
        with gzip.open(fn, 'r') as fd:
            return pp_pin_output(fd.readlines())
    else:
        with open(fn, 'r') as fd:
            return pp_pin_output(fd.readlines())

class FeatureSetsWriter:
    def __init__(self, sample_id, run_id, feature_set_name, feature_set_ver):
        self.body = {}
        self.feature_sets = {}

        self.body['sample_id'] = sample_id
        self.body['run_id'] = run_id
        self.body['feature_set_name'] = feature_set_name
        self.body['feature_set_ver'] = feature_set_ver

    def get_filename(self):
        return '{}-{}-{}-{}'.format(self.body['sample_id'], self.body['run_id'], self.body['feature_set_name'],
                                    self.body['feature_set_ver'])

    def init_feature_sets(self, feature_name):
        if feature_name not in self.feature_sets:
            self.feature_sets[feature_name] = {}

    def write_metadata(self, feature_name, meta_dict):
        self.init_feature_sets(feature_name)
        if 'feature_metadata' not in self.feature_sets[feature_name]:
            self.feature_sets[feature_name]['feature_metadata'] = []

        self.feature_sets[feature_name]['feature_metadata'].append(meta_dict)

    def write_feature_set(self, feature_name, feature_data):
        self.init_feature_sets(feature_name)
        self.feature_sets[feature_name]['feature_data'] = feature_data

    def write_feature_sets(self):
        self.body['feature_sets'] = self.feature_sets
        print json.dumps(self.body)
        with open(self.get_filename(), 'w') as fd:
            fd.write(json.dumps(self.body))
