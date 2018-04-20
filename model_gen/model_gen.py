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


class FeatureFamilyEvaluator:
    def __init__(self):
        pass


def train_evaluate_kfolds(_classifier, data, kfolds, _norm=None):
    # TODO: Why does this strip away the LabeledPoint and leave a DenseVector !?
    data_rdd = get_df(data).rdd

    model = ModelBuilder(_classifier, _norm)
    eval = ModelEvaluator(data_rdd, model)

    return eval.eval_kfolds()


def train_eval_model(input_builder, feature_nm, classifier, normalizer, kfolds):
    labelled_points = input_builder.get_lps_for_feature(feature_nm)
    kfolds_result = train_evaluate_kfolds(classifier, labelled_points, kfolds, normalizer)
    return kfolds_result


def train_eval_all(input_builder, feature_nm, csv_writer=None, kfolds=10):
    for classifier in ['svm', 'dt', 'rf']:
        for normalizer in [None, 'std']:
            results_label = '{}/{}/{}'.format(feature_nm, classifier, normalizer)
            kfolds_result = train_eval_model(input_builder, feature_nm, classifier, normalizer, kfolds)
            logging.info('{} - {}'.format(results_label, ResultStats.print_numpy(kfolds_result)))

            if csv_writer:
                csv_writer.write(ResultStats.csv_numpy(results_label, kfolds_result) + "\n")
                csv_writer.flush()


def get_sample_sets_w_replacement(folds=3):
    malware, benign = get_sample_set_from_disk(elastic_push=False)
    malware, benign = balance_classes(malware, benign)

    individual_sz = (float(1)/3)*len(malware)
    run_id = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    ret = []
    for i in range(0, folds+1):
        e_name = 'ensemble-{}'.format(run_id)
        ret.append(SampleSet(e_name, random.sample(benign, individual_sz), random.sample(malware, individual_sz)))

    return ret


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


def get_csv_handle(run_id):
    csv = open(os.path.join(config.MODELS_DIR, '{}_models.csv'.format(run_id)), 'w')
    csv.write(ResultStats.csv_header() + "\n")

    return csv


def get_train_all_features_all_models(load_sample_set, run_id):
    samples = get_sample_set(load_sample_set, run_id)

    mib = ModelInputBuilder(samples)
    mib.load_samples()

    csv = get_csv_handle(run_id)

    # TODO: Can we tag models with sample sets, so if we re-load the same sample set we don't need to re-train?
    for feature_nm in mib.get_features():
        train_eval_all(mib, feature_nm, csv)

    csv.close()


def get_train_ensemble(load_sample_sets, run_id):
    models = []

    best_by_classifier = [
        ModelMetaData('W-MemOffsetMode.MIN_REF-5000', 'dt', 'std').get(),
        ModelMetaData('W-MemOffsetMode.MIN_REF-5000', 'rf', None).get(),
        ModelMetaData('W-MemOffsetMode.MAX_REF-5000', 'svm', 'std').get()
    ]

    samples_arr = get_sample_sets_w_replacement()
    for idx, samples in enumerate(samples_arr):
        mib = ModelInputBuilder(samples)
        mib.load_samples()





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

    if not ensemble:
        get_train_all_features_all_models(loadSampleSet, run_id)
    else:
        get_train_ensemble(loadSampleSet, run_id)





