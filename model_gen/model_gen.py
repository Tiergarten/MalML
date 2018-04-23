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
from pyspark import SparkContext, SparkConf


FEATURE_FAM = 'ext-mem-rw-dump'


class SampleSetGenerator:
    @staticmethod
    def get_sample_sets_w_replacement(folds=3):
        malware, benign = get_sample_set_from_disk(elastic_push=False)
        malware, benign = balance_classes(malware, benign)

        individual_sz = (float(1) / 3) * len(malware)
        run_id = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

        ret = []
        for i in range(0, folds + 1):
            e_name = 'ensemble-{}'.format(run_id)
            ret.append(SampleSet(e_name, random.sample(benign, individual_sz), random.sample(malware, individual_sz)))

        return ret

    @staticmethod
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


class FeatureFamilyEvaluator:
    def __init__(self, run_id):
        self.run_id = run_id
        self.classifiers = ['svm', 'dt', 'rf']
        self.normalziers = [None, 'std']

    @staticmethod
    def train_eval_model(input_builder, feature_nm, classifier, normalizer):
        labelled_points = input_builder.get_lps_for_feature(feature_nm)

        # Convert generic lists into PySpark RDD
        data_rdd = get_df(labelled_points).rdd

        model = ModelBuilder(classifier, normalizer)
        evaluator = ModelEvaluator(data_rdd, model)

        return evaluator.train_eval_kfolds()

    def eval_all_for_feature(self, input_builder, feature_nm, csv_writer=None):

        for classifier in self.classifiers:
            for normalizer in self.normalziers:

                results_label = '{}/{}/{}'.format(feature_nm, classifier, normalizer)
                kfolds_result = FeatureFamilyEvaluator.train_eval_model(input_builder, feature_nm, classifier, normalizer)
                logging.info('{} - {}'.format(results_label, ResultStats.print_numpy(kfolds_result)))

                if csv_writer:
                    csv_writer.write(ResultStats.csv_numpy(results_label, kfolds_result) + "\n")
                    csv_writer.flush()

    def eval(self, sample_set):
        mib = ModelInputBuilder(sample_set)
        mib.load_samples()

        csv = self.get_csv_handle(self.run_id)

        for feature_nm in mib.get_features():
            self.eval_all_for_feature(mib, feature_nm, csv)

        csv.close()

    def get_csv_handle(self, run_id):
        csv = open(os.path.join(config.MODELS_DIR, '{}_featfam_eval.csv'.format(run_id)), 'w')
        csv.write(ResultStats.csv_header() + "\n")

        return csv


def get_train_ensemble_single_feature(load_sample_sets, run_id):

    best_by_classifier = [
        ('W-MemOffsetMode.MIN_REF-5000', ModelBuilder('dt', 'std')),
        ('W-MemOffsetMode.MIN_REF-5000', ModelBuilder('rf', None)),

        # TODO: This was not the most performant feature for svm....
        ('W-MemOffsetMode.MIN_REF-5000', ModelBuilder('svm', 'std'))
    ]

    sample_set = SampleSetGenerator.get_sample_set(load_sample_set, run_id)
    input_builder = ModelInputBuilder(sample_set)
    input_builder.load_samples()

    models = []
    for feature_nm, model_builder in best_by_classifier:
        labelled_points = input_builder.get_lps_for_feature(feature_nm)
        data_rdd = get_df(labelled_points).rdd

        train, test = get_train_test_splits(data_rdd, 2)[0]
        models.append(model_builder.build(train))

    em = EnsembleMalMlModel(models)

    results = []
    for train, test in get_train_test_splits(data_rdd, 10):
        kfolds_result = ModelEvaluator.eval(em, test)
        logging.info(kfolds_result)
        results.append(kfolds_result.to_numpy())

    logging.info(ResultStats.print_numpy(ModelEvaluator.kfolds_avg_results(results)))


if __name__ == '__main__':

    setup_logging('model_gen.log', logging.INFO)
    run_id = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    load_sample_set = None
    ensemble = False

    opts, ret = getopt.getopt(sys.argv[1:], 's:e', ['sample-set=', 'ensemble'])
    for opt, arg in opts:
        if opt in ('-s', '--sample-set'):
            load_sample_set = arg
        elif opt in ('-e', '--ensemble'):
            ensemble = True

    conf = SparkConf().setAppName('model_gen').setMaster('local[*]')
    sc = SparkContext(conf=conf)

    if not ensemble:
        samples = SampleSetGenerator.get_sample_set(load_sample_set, run_id)
        ff_eval = FeatureFamilyEvaluator(run_id)
        ff_eval.eval(samples)
    else:
        get_train_ensemble_single_feature(load_sample_set, run_id)





