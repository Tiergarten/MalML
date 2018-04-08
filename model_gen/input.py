import os
import config
import json
from pyspark.mllib.regression import LabeledPoint
from mg_common import *
from common import *
import logging
from datetime import datetime

class ModelInputBuilder:
    def __init__(self, sample_set):
        self.sample_set = sample_set

        self.agg_lps = []

    def load_samples(self):
        self.agg_lps = self.get_features_lps(self.sample_set.get('benign'), SampleLabelPredictor.BENIGN)
        self.agg_lps.update(self.get_features_lps(self.sample_set.get('malware'), SampleLabelPredictor.MALWARE))


    # TODO: Not all samples are going to have _all_ feature sets (Size constraints etc)
    def get_features(self):
        features = []

        for sample in self.agg_lps:
            for feature_set_n, lps in self.agg_lps[sample]:
                if feature_set_n not in features:
                    features.append(feature_set_n)

        return features

    def get_lps_for_feature(self, feature_name):
        ret = []
        for sample in self.agg_lps:
            for feature_set_n, lps in self.agg_lps[sample]:
                if feature_set_n == feature_name:
                    ret.append(lps)

        return ret

    def get_feature_labeled_points(self, feature_json, label):
        ret = []

        for feature_set in feature_json['feature_sets']:
            feature_data = feature_json['feature_sets'][feature_set]['feature_data']
            ret.append((feature_set, LabeledPoint(label, [float(f) for f in feature_data])))

        return ret

    def get_feature_json(self, sample):
        with open(os.path.join(config.FEATURES_DIR, sample, str(0), 'ext-mem-rw-dump-0.0.1.json'), 'r') as fd:
            return json.load(fd)

    def get_features_lps(self, samples, label):
        ret = {}
        for s in samples:
            j = self.get_feature_json(s)

            features = self.get_feature_labeled_points(j, label)
            if len(features) > 0:
                ret[s] = features

        return ret


def get_feature_files():
    return [os.path.join(config.FEATURES_DIR, f, '0', 'ext-mem-rw-dump-0.0.1.json')
            for f in os.listdir(config.FEATURES_DIR) if not f.startswith('.')]


def get_sample_set_from_disk(balanced=True, elastic_push=False):
    malware = []
    benign = []

    total_features = get_feature_files()
    logging.info('total feature files: {}'.format(len(total_features)))

    elastic_wrote = 0
    for f in total_features:
        with open(f, 'r') as fd:
            md = json.load(fd)

        if len(md['feature_sets']) == 0:
            continue

        if elastic_push:
            _id = '{}-{}-{}'.format(md['sample_id'], md['uuid'], md['run_id'])
            get_elastic().index(index=config.ES_CONF_FEATURES[0], doc_type=config.ES_CONF_FEATURES[1],
                                body=json.dumps(md), id=_id)
            elastic_wrote += 1

        ds = DetonationSample(md['sample_id'])
        label = SampleLabelPredictor(ds).get_explicit_label()

        if label == SampleLabelPredictor.MALWARE:
            malware.append(ds.sample)
        elif label == SampleLabelPredictor.BENIGN:
            benign.append(ds.sample)
        else:
            logging.warn('unknown label: {}'.format(label))

    logging.info('benign: {}, malware: {}, total: {}'.format(len(benign), len(malware),
                                                             len(benign) + len(malware)))
    if balanced:
        if len(malware) > len(benign):
            malware = malware[0:len(benign)]
        elif len(benign) > len(malware):
            benign = benign[0:len(malware)]

        logging.info('balanced: benign: {}, malware: {}'.format(len(benign), len(malware)))

    logging.info('wrote to elastic: {}'.format(elastic_wrote))

    return SampleSet('all-{}'.format(datetime.now()), benign, malware)