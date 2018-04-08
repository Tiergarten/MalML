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

    # TODO: This gets _ALL_ samples, we need to be more selective
    all_samples = get_sample_set_from_disk(balanced=True, elastic_push=False)

    mib = ModelInputBuilder(all_samples)
    mib.load_samples()

    # TODO: Can we tag models with sample sets, so if we re-load the same sample set we don't need to re-train?

    for feature_nm in mib.get_features():
        data = mib.get_lps_for_feature(feature_nm)

        for classifier in ['svm', 'rf']:
            for normalizer in [None, 'std']:
                logging.info('{}/{}/{} - {}'.format(feature_nm, classifier, normalizer,
                                            train_evaluate_kfolds(classifier, data, 3, normalizer)))
