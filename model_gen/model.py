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
from pyspark.mllib.tree import RandomForest, RandomForestModel


def get_train_test_splits(labelled_rdd, split_n=10):
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

    def persist_model(self, model, fn):
        common.create_dirs_if_not_exist(fn)
        model.save(SparkContext.getOrCreate(), fn)
        logging.info('wrote {}'.format(fn))

    def save_to_disk(self):
        model_uuid = uuid.uuid4().hex
        model_dir = os.path.join(config.MODELS_DIR, 'raw', model_uuid)

        # TODO: we're going to need to write out json saying it was std scaled + the train data...
        """
        if self.normalizer is not None:
            self.persist_model(self.normalizer, os.path.join(model_dir, 'norm_0'))
"""
        self.persist_model(self.model, os.path.join(model_dir, 'model_0'))

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

    def __str__(self):
        return 'MalMlModel({}/{})'.format(str(self.classifier), str(self.normalizer))


class EnsembleMalMlModel:
    def __init__(self, models):
        self.models = models

    def evaluate(self, input_vector):
        votes = []
        for m in self.models:
            votes.append(m.evaluate(input_vector))

        ret = max(set(votes), key=votes.count)
        #logging.info('votes: {}, ret: {}'.format(votes, ret))
        return ret

    def ensemble_name(self):
        return'EnsembleMalMlModel(' + ','.join([str(m) for m in self.models]) + ')'

    def __str__(self):
        return 'EnsembleMalMlModel('+str(len(self.models))+')'


class SVMClassifier:
    def __init__(self):
        pass

    def train(self, training_data):
        return SVMWithSGD.train(training_data)

    def __str__(self):
        return 'SVMClassifier'


class DTClassifier:
    def __init__(self):
        pass

    def train(self, training_data):
        return DecisionTree.trainClassifier(training_data, 2, categoricalFeaturesInfo={},
                                            impurity='gini', maxDepth=5, maxBins=32)

    def __str__(self):
        return 'DTClassifier'


class RFClassifier:
    def __init__(self):
        pass

    def train(self, training_data):
        return RandomForest.trainClassifier(training_data, numClasses=2, categoricalFeaturesInfo={},
                                 numTrees=6, featureSubsetStrategy="auto",
                                 impurity='gini', maxDepth=5, maxBins=32)

    def __str__(self):
        return 'RFClassifier'


class StandardScalerNormalizer:
    def __init__(self):
        self.normalizer = None

    def norm_train(self, train_data):
        train_features = train_data.map(lambda lp: lp.features)
        self.normalizer = StandardScaler().fit(train_features)

        # TODO: This can't be efficient...
        #return train_data.map(lambda lp: lp.label).zip(self.norm(train_features)).map(lambda r: LabeledPoint(r[0], r[1]))
        labels = train_data.map(lambda lp: lp.label).collect()
        features = self.norm(train_features).collect()
        return get_df(zip(labels, features)).rdd.map(lambda r: LabeledPoint(r[0], r[1]))

    def norm(self, data):
        return self.normalizer.transform(data)

    def __str__(self):
        return 'StandardScaler'


class ModelEvaluator:
    def __init__(self, dataset, model_builder):
        self.dataset = dataset
        self.model_builder = model_builder
        self.results = []

    def train_eval_kfolds(self, kfolds=5):
        train_test = get_train_test_splits(self.dataset, kfolds)

        fold = 0
        for train, test in train_test:

            model = self.model_builder.build(train)

            results = ModelEvaluator.eval(model, test)
            logging.info('[{}] {}'.format(fold, results))

            self.results.append(results.to_numpy())
            fold += 1

        return ModelEvaluator.kfolds_avg_results(self.results)

    @staticmethod
    def kfolds_avg_results(results):
        return np.average(results, axis=0)

    @staticmethod
    def eval(model, test):
        output = []
        for lp in test.collect():
                output.append((lp.label, float(model.evaluate(lp.features))))

        return ResultStats(model, output)


class ModelBuilder:
    classifier_map = {
        'svm': SVMClassifier,
        'dt': DTClassifier,
        'rf': RFClassifier
    }

    normalizer_map = {
        'std': StandardScalerNormalizer,
        None: None
    }

    def __init__(self, classifier, normalizer):
        self.classifier = classifier
        self.normalizer = normalizer

    def build(self, train_data):
        model = MalMlModel(train_data, ModelBuilder.classifier_map[self.classifier](),
                           None if self.normalizer is None else ModelBuilder.normalizer_map[self.normalizer]())

        model.build()
        return model

    def __str__(self):
        return '{}/{}'.format(self.classifier, self.normalizer)
