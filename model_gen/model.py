from pyspark.mllib.classification import SVMWithSGD
from pyspark.mllib.tree import DecisionTree
import common
import logging
import config
import os
from pyspark import SparkContext
import uuid
from mg_common import ResultStats, get_df
import numpy as np
import time
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.feature import StandardScaler


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


class MalMlModel:
    def __init__(self, training_data, classifier, normalizer=None):
        self.training_data = training_data
        self.classifier = classifier
        self.normalizer = normalizer
        self.model = None

    def load_from_disk(self):
        pass

    def save_to_disk(self):
        model_uuid = uuid.uuid4().hex
        model_path = os.path.join(config.MODELS_DIR, 'raw', model_uuid, 'model_0')

        common.create_dirs_if_not_exist(model_path)
        self.model.save(SparkContext.getOrCreate(), model_path)

        logging.info('wrote {}'.format(model_path))

    def build(self):
        train_lp = self.training_data.map(lambda r: LabeledPoint(r[1], r[0]))

        if self.normalizer is not None:
            train_lp = self.normalizer.norm_train(train_lp)

        self.model = self.classifier.train(train_lp)

    def evaluate(self, input_vector):
        if self.normalizer is not None:
            return self.model.predict(self.normalizer.norm(input_vector))
        else:
            return self.model.predict(input_vector)


class SVMClassifier:
    def __init__(self):
        pass

    def train(self, training_data):
        return SVMWithSGD.train(training_data)


class RFClassifier:
    def __init__(self):
        pass

    def train(self, training_data):
        return DecisionTree.trainClassifier(training_data, 2, categoricalFeaturesInfo={},
                                            impurity='gini', maxDepth=5, maxBins=32)


class StandardScalerNormalizer:
    def __init__(self):
        self.normalizer = None

    def norm_train(self, train_data):
        train_features = train_data.map(lambda lp: lp.features)
        self.normalizer = StandardScaler().fit(train_features)

        # TODO: This can't be efficient...
        labels = train_data.map(lambda lp: lp.label).collect()
        features = self.norm(train_features).collect()
        return get_df(zip(labels, features)).rdd.map(lambda r: LabeledPoint(r[0], r[1]))

    def norm(self, data):
        return self.normalizer.transform(data)


class MalMlFeatureEvaluator:
    def __init__(self, dataset, classifier, normalizer, k=5):
        self.dataset = dataset
        self.classifier = classifier
        self.normalizer = normalizer
        self.folds = k
        self.models = []
        self.results = []

    def eval(self):
        train_test = get_train_test_split(self.dataset, self.folds)
        for train, test in train_test:
            model = MalMlModel(train, self.classifier, self.normalizer)
            model.build()

            self.models.append(model)

            output = []
            for lp in test.collect():
                output.append((lp.label, float(model.evaluate(lp.features))))

            results = ResultStats(self.classifier.__class__.__name__, output)
            logging.info(results)
            self.results.append(results.to_numpy())

        return ResultStats.print_numpy(np.average(self.results, axis=0))
