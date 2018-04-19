from pyspark.sql.session import SparkSession
from pyspark import SparkContext

from elasticsearch import Elasticsearch

import json

import numpy as np

from pyspark.mllib.evaluation import BinaryClassificationMetrics

import config

import logging

def get_elastic():
    return Elasticsearch()


def get_df(data):
    return SparkSession.builder.getOrCreate().createDataFrame(data)


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

    def div(self, numerator, denominator):
        if denominator == 0:
            return 0

        return float(numerator) / denominator

    def get_accuracy(self):
        return self.div(self.tp + self.fn, self.get_total_measures())

    def get_precision(self):
        return self.div(self.tp, self.tp + self.fp)

    def get_recall(self):
        return self.div(self.tp, self.tp + self.fn)

    def get_specificity(self):
        return self.div(self.tn, self.tn+self.fp)

    def get_f1_score(self):
        return 2 * self.div(self.get_recall() * self.get_precision(), self.get_recall() + self.get_precision())

    def get_area_under_roc(self):
        # we store (label, score) - this expects (score, label)
        return BinaryClassificationMetrics(get_df([(x[1], x[0]) for x in self.results]).rdd).areaUnderROC

    def to_numpy(self):
        return np.array(
            [self.get_accuracy(), self.get_precision(), self.get_recall(), self.get_specificity(), self.get_f1_score(),
             self.get_area_under_roc()])

    @staticmethod
    def print_numpy(np):
        return "[Accuracy: %f, Precision: %f, Recall: %f, Specificity: %f, F1: %f AuROC: %f]" % (
        np[0], np[1], np[2], np[3], np[4], np[5])


    @staticmethod
    def csv_header():
        return "feature,accuracy,precision,recall,specificity,f1-score,AuROC"

    @staticmethod
    def csv_numpy(feature_nm, np):
        return "{},{},{},{},{},{},{}".format(feature_nm, np[0], np[1], np[2], np[3], np[4], np[5])

    def __str__(self):
        return "[%s][TP:%d TN:%d FP:%d FN:%d] [Acc: %f] [Prec: %f] [Recall: %f] [Specif: %f] [F1: %f] [AuROC:%f]" % (
        self.label, self.tp, self.tn, self.fp, self.fn, self.get_accuracy(), self.get_precision(), self.get_recall(),
        self.get_specificity(), self.get_f1_score(), self.get_area_under_roc())

    def to_json(self):
        return json.dumps({"accuracy": self.get_accuracy(), "precision": self.get_precision(), "recall": self.get_recall(),
                           "specificity": self.get_specificity(), "f1_score": self.get_f1_score(), "AuROC": self.get_area_under_roc()})


class SampleSet:
    def __init__(self, name, benign, malware, tags=[]):
        self.__version__ = '0.0.1'
        self.json_body = {
            'name': name,
            'tags': tags,
            'data': {
                'benign': benign,
                'malware': malware
            }
        }

    def get(self, str):
        return self.json_body['data'][str]

    def write(self, fn):
        with open(fn, 'w') as fd:
            fd.write(json.dumps(self.json_body))

    @staticmethod
    def from_file(self, fn):
        with open(fn, 'r') as fd:
            j = json.loads(fd.read())

        return SampleSet(j['name'], j['data']['benign'], j['data']['malware'])

    def __str__(self):
        return '{}: malware:{}, benign:{}'.format(self.json_body['name'],
                                                  len(self.json_body['data']['benign']),
                                                  len(self.json_body['data']['malware']))


class SampleLabelPredictor:
    MALWARE = 1.0
    BENIGN = 0.0

    def __init__(self, detonation_sample, pcount_min=5, pcount_perc=None):
        self.sample = detonation_sample
        self.sample_json = detonation_sample.get_metadata()

        self.pcount_min = pcount_min
        self.pcount_perc = pcount_perc

    def get_scan_data(self):
        if 'vti' in self.sample_json and 'scans' in self.sample_json['vti']:
            return self.sample_json['vti']['scans']
        elif 'existing' in self.sample_json:
            return self.sample_json['existing']['scans']
        else:
            return None

    def vti_predict_label(self):
        scan_data = self.get_scan_data()

        if scan_data is None:
            #logging.error('unable to get scan data for {}!'.format(self.sample.sample))
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
        if 'label' in self.sample_json.keys():
            if self.sample_json['label'] == 'malware':
                return SampleLabelPredictor.MALWARE
            elif self.sample_json['label'] == 'benign':
                return SampleLabelPredictor.BENIGN
            else:
                raise Exception('unknown Label!)')
        else:
            return None

    def get_label(self, vti_only=False, explicit_only=False):
        explicit_label = self.get_explicit_label()
        vti_label = self.vti_predict_label()

        if vti_only is True:
            return vti_label
        elif explicit_only is True:
            return explicit_label

        if explicit_label is not None:
            return explicit_label
        else:
            return vti_label
