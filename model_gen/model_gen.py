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

FEATURE_FAM = 'ext-mem-rw-dump'


def train_evaluate_kfolds(_classifier, data, kfolds, _norm=None):
    # TODO: Why does this strip away the LabeledPoint and leave a DenseVector !?
    data_rdd = get_df(data).rdd

    if _classifier == 'svm':
        classifier = SVMClassifier()
    elif _classifier == 'rf':
        classifier = RFClassifier()

    if _norm == 'std':
        norm = StandardScalerNormalizer()
    else:
        norm = None

    eval = MalMlFeatureEvaluator(data_rdd, classifier, norm)
    return eval.eval()


if __name__ == '__main__':

    setup_logging('model_gen.log')

    # Generate run-id
    run_id = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    # TODO: This gets _ALL_ samples, we need to be more selective
    sample_set = get_sample_set_from_disk(balanced=True, elastic_push=False)
    sample_set.write(os.path.join(config.MODELS_DIR, '{}_sampleset.json'.format(run_id)))

    mib = ModelInputBuilder(sample_set)
    mib.load_samples()

    csv = open(os.path.join(config.MODELS_DIR, '{}_models.csv'.format(run_id)), 'w')
    csv.write(ResultStats.csv_header()+"\n")

    # TODO: Can we tag models with sample sets, so if we re-load the same sample set we don't need to re-train?
    for feature_nm in mib.get_features():
        data = mib.get_lps_for_feature(feature_nm)

        for classifier in ['svm', 'rf']:
            for normalizer in [None, 'std']:
                feature_desc = '{}/{}/{}'.format(feature_nm, classifier, normalizer)
                kfolds_result = train_evaluate_kfolds(classifier, data, 3, normalizer)

                logging.info('{} - {}'.format(feature_desc, ResultStats.print_numpy(kfolds_result)))
                csv.write(ResultStats.csv_numpy(feature_desc, kfolds_result)+"\n")
                csv.flush()

    csv.close()
