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
from pyspark.mllib.feature import StandardScaler
from model import *
from input import *
import datetime
import getopt


FEATURE_FAM = 'ext-mem-rw-dump'


def train_evaluate_kfolds(_classifier, data, kfolds, _norm=None):
    # TODO: Why does this strip away the LabeledPoint and leave a DenseVector !?
    data_rdd = get_df(data).rdd

    if _classifier == 'svm':
        classifier = SVMClassifier()
    elif _classifier == 'dt':
        classifier = DTClassifier()
    elif _classifier == 'rf':
        classifier = RFClassifier()

    if _norm == 'std':
        norm = StandardScalerNormalizer()
    else:
        norm = None

    eval = MalMlFeatureEvaluator(data_rdd, classifier, norm, kfolds)
    return eval.eval()


def produce_all_models(feature_lp, csv_writer, kfolds=10):
    for classifier in ['svm', 'dt', 'rf']:
        for normalizer in [None, 'std']:
            feature_desc = '{}/{}/{}'.format(feature_nm, classifier, normalizer)
            kfolds_result = train_evaluate_kfolds(classifier, feature_lp, kfolds, normalizer)

            logging.info('{} - {}'.format(feature_desc, ResultStats.print_numpy(kfolds_result)))
            csv_writer.write(ResultStats.csv_numpy(feature_desc, kfolds_result) + "\n")
            csv.flush()

def get_sample_sets(folds=3):
    malware, benign = get_sample_set_from_disk(elastic_push=False)
    malware, benign = balance_classes(malware, benign)

    ret = []
    for i in range(0, folds+1):
        ret.append()

def get_sample_set(sample_set, run_id):
    if sample_set is None:
        # TODO: This gets _ALL_ samples, we need to be more selective
        malware, benign = get_sample_set_from_disk(elastic_push=False)
        malware, benign = balance_classes(malware, benign)

        sample_set = SampleSet(run_id, benign, malware)
        fn = '{}_sampleset.json'.format(run_id)
        sample_set.write(fn)

        logging.info('wrote {}'.format(fn))
    else:
        sample_set = SampleSet.from_file(sample_set)

    logging.info('Got sample set {}'.format(sample_set))

    return sample_set


if __name__ == '__main__':

    setup_logging('model_gen.log', logging.INFO)
    run_id = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    loadSampleSet = None
    ensemble = False

    opts, ret = getopt.getopt(sys.argv[1:], 's:e', ['sample-set=', 'ensemble'])
    for opt, arg in opts:
        if opt in ('-s', '--sample-set'):
            loadSampleSet = arg
        elif opt in ('-e', '--ensemble'):
            ensemble = True

    samples = get_sample_set(loadSampleSet, run_id)
    mib = ModelInputBuilder(samples)
    mib.load_samples()

    csv = open(os.path.join(config.MODELS_DIR, '{}_models.csv'.format(run_id)), 'w')
    csv.write(ResultStats.csv_header()+"\n")

    # TODO: Can we tag models with sample sets, so if we re-load the same sample set we don't need to re-train?
    for feature_nm in mib.get_features():
        produce_all_models(mib.get_lps_for_feature(feature_nm), csv)

    csv.close()

