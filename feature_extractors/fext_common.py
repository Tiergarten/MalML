import os
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


def create_dirs_if_not_exist(path):
    try:
        os.makedirs(os.path.dirname(path))
    except:
        pass


class FeatureSetsWriter:
    def __init__(self, output_dir, sample_id, run_id, feature_set_name, feature_set_ver):
        self.body = {}
        self.feature_sets = {}

        self.output_dir = output_dir
        self.body['sample_id'] = sample_id
        self.body['run_id'] = str(run_id)
        self.body['feature_set_name'] = feature_set_name
        self.body['feature_set_ver'] = feature_set_ver


    def get_filename(self):
        fn = '{}-{}.json'.format(self.body['feature_set_name'], self.body['feature_set_ver'])
        return os.path.join(self.output_dir, self.body['sample_id'], self.body['run_id'], fn)

    def already_exists(self):
        return os.path.exists(self.get_filename())

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
        create_dirs_if_not_exist(self.get_filename())
        with open(self.get_filename(), 'w') as fd:
            fd.write(json.dumps(self.body, indent=4, sort_keys=True))
