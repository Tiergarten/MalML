from common import *
import random
from mg_common import *
import config
from pyspark.mllib.regression import LabeledPoint
import json
from pyspark.sql.session import SparkSession
import time
from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
from pyspark.mllib.util import MLUtils
from pyspark.mllib.classification import SVMWithSGD, SVMModel
from pyspark.mllib.evaluation import BinaryClassificationMetrics
import numpy as np

FEATURE_FAM = 'ext-mem-rw-dump'


class UploadSearch:
    def __init__(self, extractor_pack=None, feature_family=None, sample=None):
        self._extractor_pack = extractor_pack
        self._feature_family = feature_family
        self._sample = sample

    @staticmethod
    def s():
        return Search(using=get_elastic(), index=config.ES_CONF_UPLOADS[0], doc_type=config.ES_CONF_UPLOADS[1])

    def search(self):
        ret = UploadSearch.s()
        if self._extractor_pack is not None:
            ret = ret.filter('match', extractor_pack=self._extractor_pack)
        if self._feature_family is not None:
            ret = ret.filter('match', feature_family=self._feature_family)
        if self._sample is not None:
            ret = ret.filter('match', sample=self._sample)

        return ret.execute()


class SampleSearch:
    def __init__(self, label=None, arch=None, source=None, sample=None):
        self._label = label
        self._arch = arch
        self._source = source
        self._sample = sample

    @staticmethod
    def s():
        return Search(using=get_elastic(), index=config.ES_CONF_SAMPLES[0], doc_type=config.ES_CONF_SAMPLES[1])

    def search(self):
        ret = SampleSearch.s()
        if self._label is not None:
            ret = ret.filter('match', label=self._label)
        if self._arch is not None:
            ret = ret.filter('match', arch=self._arch)
        if self._source is not None:
            ret = ret.filter('match', source=self._source)
        if self._sample is not None:
            ret = ret.filter('match', sample=self._sample)

        return ret.execute()


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


def get_features():
    return [os.path.join(config.FEATURES_DIR, f, '0', 'ext-mem-rw-dump-0.0.1.json')
            for f in os.listdir(config.FEATURES_DIR) if not f.startswith('.')]

class ModelInputBuilder:
    @staticmethod
    def get_feature_labeled_points(feature_json, label):
        ret = []

        for feature_set in feature_json['feature_sets']:
            feature_data = feature_json['feature_sets'][feature_set]['feature_data']
            ret.append((feature_set, LabeledPoint(label, [float(f) for f in feature_data])))

        return ret

    @staticmethod
    def get_feature_json(sample):
        #es_s = UploadSearch(sample=sample).search().hits[0]
        #DetonationUpload(config.UPLOADS_DIR, es_s.sample, es_s.uuid, [es_s.run_id]).get_metadata(es_s.run_id)
        with open(os.path.join(config.FEATURES_DIR, sample, str(0), 'ext-mem-rw-dump-0.0.1.json'), 'r') as fd:
            return json.load(fd)

    @staticmethod
    def get_feature_lps(sample, label):
        j = ModelInputBuilder.get_feature_json(sample)
        return ModelInputBuilder.get_feature_labeled_points(j, label)

    @staticmethod
    def get_features_lps(samples, label):
        ret = {}
        for s in samples:
            features = ModelInputBuilder.get_feature_lps(s, label)
            if len(features) > 0:
                ret[s] = features

        return ret

    @staticmethod
    def get_features_lps_from_samples(benign, malware):
        b_lps = ModelInputBuilder.get_features_lps(benign, SampleLabelPredictor.BENIGN)
        m_lps = ModelInputBuilder.get_features_lps(malware, SampleLabelPredictor.MALWARE)

        return b_lps, m_lps

class SampleLabelPredictor:
    MALWARE = 1.0
    BENIGN = 0.0

    def __init__(self, detonation_sample, pcount_min=5, pcount_perc=None, vti_only=False):
        self.sample = detonation_sample
        self.sample_json = detonation_sample.get_metadata()

        self.pcount_min = pcount_min
        self.pcount_perc = pcount_perc
        self.vti_only = vti_only

    def get_scan_data(self):
        if 'vti' in self.sample_json and 'scans' in self.sample_json['vti']:
            return self.sample_json['vti']['scans']
        elif 'existing_metadata' in self.sample_json:
            return self.sample_json['existing_metadata']['scans']
        else:
            return None

    def vti_predict_label(self):
        scan_data = self.get_scan_data()

        if scan_data is None:
            return None

        detected = 0
        for i in scan_data:
            if scan_data[i]['detected'] is True:
                detected += 1

        if self.pcount_perc is not None:
            if (detected / float(len(scan_data)) * 100) > self.pcount_perc:
                return SampleLabelPredictor.MALWARE
            else:
                return SampleLabelPredictor.BENIGN
        else:
            if detected > self.pcount_min:
                return SampleLabelPredictor.MALWARE
            else:
                return SampleLabelPredictor.BENIGN

    def get_explicit_label(self):
        if 'label' in self.sample_json:
            return self.sample_json['label']
        else:
            return None

    def get_label(self):
        explicit_label = self.get_explicit_label()
        vti_label = self.vti_predict_label()

        if self.vti_only is True and vti_label is not None:
            return vti_label

        return SampleLabelPredictor.MALWARE if explicit_label == 'malware' else SampleLabelPredictor.BENIGN
    
    
def get_features_from_disk():
    total_features = get_features()
    malware = []
    benign = []

    for f in total_features:
        with open(f, 'r') as fd:
            md = json.load(fd)

        # TODO: Skip no data...

        ds = DetonationSample(md['sample_id'])
        label = SampleLabelPredictor(ds, vti_only=True).get_label()

        if label == SampleLabelPredictor.MALWARE:
            malware.append(ds.sample)
        else:
            benign.append(ds.sample)

    logging.info('source, benign: {}, malware: {}'.format(len(benign), len(malware)))

    return benign, malware

def get_feature_set_from_lps(fset_nm, lps):
    ret = []

    for sample in lps:
        for feature_set_nm, data in lps[sample]:
            if feature_set_nm == fset_nm:
                ret.append((sample, data))

    return ret

def get_df(data):
    return SparkSession.builder.getOrCreate().createDataFrame(data)

def get_train_test_split(labelled_rdd, split_n=10):
    ret = []
    splits = labelled_rdd.randomSplit([float(1) / split_n] * split_n, seed=int(time.time()))
    for i in range(0, split_n):
        test = splits[i]

        train_list = list(splits)
        train_list.remove(test)

        train_rdd = train_list[0]
        for train_idx in range(1, len(train_list)):
            train_rdd.union(train_list[train_idx])

        ret.append((test, train_rdd))

    return ret

def train_classifier_and_measure(ctype, training_data, test_data):
    if ctype == "svm":
        model = SVMWithSGD.train(training_data, iterations=100)
    elif ctype == "rf":
        model = DecisionTree.trainClassifier(training_data, 2, categoricalFeaturesInfo={}, impurity='gini', maxDepth=5,
                                             maxBins=32)

    output = []
    for lp in test_data.collect():
        output.append((lp.label, float(model.predict(lp.features))))

    return output

class ResultStats:
    def __init__(self, label, results):
        self.results = results

        self.tp = self.get_result_cnt(results, actual=1.0, predicted=1)
        self.tn = self.get_result_cnt(results, actual=0.0, predicted=0)

        self.fp = self.get_result_cnt(results, actual=0.0, predicted=1)
        self.fn = self.get_result_cnt(results, actual=1.0, predicted=0)

        self.total_measures = self.tp + self.tn + self.fp + self.fn

        self.label = label

    @staticmethod
    def get_result_cnt(results, actual, predicted):
        return len(filter(lambda pair: pair[0] == actual and pair[1] == predicted, results))

    def get_total_measures(self): return self.tp + self.tn + self.fp + self.fn

    def get_accuracy(self):
        return float(self.tp + self.tn) / self.get_total_measures() if self.tp + self.tn > 0 else 0

    def get_precision(self):
        return float(self.tp) / (self.tp + self.tn) if self.tp > 0 else 0

    def get_recall(self):
        return float(self.tp) / self.get_total_measures() if self.tp > 0 else 0

    def get_specificity(self):
        return float(self.tn) / self.get_total_measures() if self.tn > 0 else 0

    def get_f1_score(self):
        return 2 * float(self.get_recall() + self.get_precision()) / self.get_recall() + self.get_precision() \
            if self.get_recall() + self.get_precision() > 0 else 0

    def get_area_under_roc(self):
        return BinaryClassificationMetrics(get_df(self.results).rdd).areaUnderROC

    def to_numpy(self):
        return np.array(
            [self.get_accuracy(), self.get_precision(), self.get_recall(), self.get_specificity(), self.get_f1_score(),
             self.get_area_under_roc()])

    @staticmethod
    def print_numpy(np):
        return "[Accuracy: %f, Precision: %f, Recall: %f, Specificity: %f, F1: %f AuROC: %f]" % (
        np[0], np[1], np[2], np[3], np[4], np[5])

    def __str__(self):
        return "[%s][TP:%d TN:%d FP:%d FN:%d] [Acc: %f] [Prec: %f] [Recall: %f] [Specif: %f] [F1: %f] [AuROC:%f]" % (
        self.label, self.tp, self.tn, self.fp, self.fn, self.get_accuracy(), self.get_precision(), self.get_recall(),
        self.get_specificity(), self.get_f1_score(), self.get_area_under_roc())

if __name__ == '__main__':
    """
    with open('features/615cc5670435e88acb614c467d6dc9b09637f917f02f3b14cd8460d1ac6058ec/2/ext-mem-rw-dump-0.0.1.json') as fd:
        j = json.loads(fd.read())
    print ModelBuilder.get_feature_labeled_points(j, 1.0)

    print SampleLabelPredicter(DetonationSample('ff808d0a12676bfac88fd26f955154f8884f2bb7c534b9936510fd6296c543e8')).get_label()
"""

    setup_logging('model_gen.log')

    # TODO: This gets _ALL_ samples, we need to be more selective
    benign, malware = get_features_from_disk()

    blps, mlps = ModelInputBuilder.get_features_lps_from_samples(benign, malware)

    logging.info('source dataset: benign: {}, malware: {}'.format(len(blps), len(mlps)))

    feature_sets = [x[0] for x in [f for f in blps[blps.keys()[0]]]]
    for i in feature_sets:

        # (sample_nm, LabeledPoint(1.0, [features])
        data = get_feature_set_from_lps(i, blps) + get_feature_set_from_lps(i, mlps)

        # TODO: Why does this strip away the LabeledPoint and leave a DenseVector !?
        data_df = get_df(data)

        count = 0
        results = []
        for train, test in get_train_test_split(data_df.rdd, 10):

            # TODO: Why is it sample, Row(DenseVector... ? .... WTF IS THIS >>
            model_predictions = train_classifier_and_measure('svm',
                                                             train.map(lambda r: LabeledPoint(r[1][1], r[1][0])),
                                                             test.map(lambda r: LabeledPoint(r[1][1], r[1][0])))

            iteration_results = ResultStats('svm', model_predictions)
            results.append(iteration_results.to_numpy())
            logging.info("[%s] [F%d] [train:%d, test:%d] %s", i, count, train.count(), test.count(), iteration_results)
            count += 1

        logging.info("[%s] [K%d] %s", i, 10, ResultStats.print_numpy(np.average(results, axis=0)))
