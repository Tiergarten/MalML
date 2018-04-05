from common import *
import random
from mg_common import *
import config
from pyspark.mllib.regression import LabeledPoint
import json
import time
from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
from pyspark.mllib.util import MLUtils
from pyspark.mllib.classification import SVMWithSGD, SVMModel
import numpy as np
from datetime import datetime
import uuid

FEATURE_FAM = 'ext-mem-rw-dump'


def get_labelled_sample_set(_feature_fam, _label, count, exclude=[]):
    ret = []
    labelled_samples = SampleSearch(label=_label).search()

    for _sample in labelled_samples:
        for result in UploadSearch(extractor_pack='pack-1', sample=_sample).search():
            ret.append(result)

    if len(ret) >= count:
        return random.sample(ret, count)
    else:
        return []


def get_feature_files():
    return [os.path.join(config.FEATURES_DIR, f, '0', 'ext-mem-rw-dump-0.0.1.json')
            for f in os.listdir(config.FEATURES_DIR) if not f.startswith('.')]


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

    def balance_datasets(self, a, b):
        return a, b # TODO: Fix me
        if len(a.keys()) > len(b.keys()):
            return a[0:len(b)], b
        else:
            return a, b[0:len(a)]

    
def get_sample_set_from_disk(balanced=True):
    malware = []
    benign = []

    total_features = get_feature_files()

    for f in total_features:
        with open(f, 'r') as fd:
            md = json.load(fd)

        if len(md['feature_sets']) == 0:
            continue

        ds = DetonationSample(md['sample_id'])
        label = SampleLabelPredictor(ds).get_label()

        if label == SampleLabelPredictor.MALWARE:
            malware.append(ds.sample)
        else:
            benign.append(ds.sample)

    if balanced:
        if len(malware) > len(benign):
            malware = malware[0:len(benign)]
        elif len(benign) > len(malware):
            benign = benign[0:len(malware)]

    logging.info('feature families: total: {}, good: {}, benign: {}, malware: {} (balanced: {})'
                 .format(len(total_features), len(malware)+len(benign), len(benign), len(malware), balanced))

    return SampleSet('all-{}'.format(datetime.now()), benign, malware)


# TODO: Review this
def get_train_test_split(labelled_rdd, split_n=10):
    ret = []
    splits = labelled_rdd.randomSplit([float(1) / split_n] * split_n, seed=int(time.time()))
    for i in range(0, split_n):
        test = splits[i]

        train_list = list(splits)
        train_list.remove(test)

        train_rdd = train_list[0]
        for train_idx in range(1, len(train_list)):
            train_rdd = train_rdd.union(train_list[train_idx])

        ret.append((train_rdd, test))

    return ret


def train_classifier_and_measure(ctype, training_data, test_data):
    # TODO: Check it doesn't already exist

    if ctype == "svm":
        model = SVMWithSGD.train(training_data, iterations=100)
    elif ctype == "rf":
        model = DecisionTree.trainClassifier(training_data, 2, categoricalFeaturesInfo={}, impurity='gini', maxDepth=5,
                                             maxBins=32)

    model_uuid = uuid.uuid4().hex
    model_path = os.path.join(config.MODELS_DIR, 'raw', model_uuid, 'model_0')

    create_dirs_if_not_exist(model_path)
    model.save(SparkContext.getOrCreate(), model_path)
    logging.info('wrote {}'.format(model_path))

    # TODO: Write metadata: (train)sampleset, normalization, hyper parameters

    output = []
    for lp in test_data.collect():
        output.append((lp.label, float(model.predict(lp.features))))

    return output


def train_evaluate_kfolds(classifier, data, kfolds):
    # TODO: Why does this strip away the LabeledPoint and leave a DenseVector !?
    data_df = get_df(data)

    count = 0
    results = []
    for train, test in get_train_test_split(data_df.rdd, kfolds):
        model_predictions = train_classifier_and_measure(classifier,
                                                         # Mapping back to LabeledPoint
                                                         train.map(lambda r: LabeledPoint(r[1], r[0])),
                                                         test.map(lambda r: LabeledPoint(r[1], r[0])))

        iteration_results = ResultStats(classifier, model_predictions)
        results.append(iteration_results.to_numpy())
        logging.info("[%s][%s][F%d] [train:%d, test:%d] %s", feature_nm, classifier, count, train.count(), test.count(),
                     iteration_results)
        count += 1

    logging.info("[%s][%s][K%d] %s", feature_nm, classifier, kfolds, ResultStats.print_numpy(np.average(results, axis=0)))

if __name__ == '__main__':

    setup_logging('model_gen.log')

    # TODO: This gets _ALL_ samples, we need to be more selective
    all_samples = get_sample_set_from_disk()
    mib = ModelInputBuilder(all_samples)
    mib.load_samples()

    # TODO: Can we tag models with sample sets, so if we re-load the same sample set we don't need to re-train?

    for feature_nm in mib.get_features():
        data = mib.get_lps_for_feature(feature_nm)

        for classifier in ['svm', 'rf']:
            train_evaluate_kfolds(classifier, data, 5)


