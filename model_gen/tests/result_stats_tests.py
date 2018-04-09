import unittest
from model_gen.mg_common import ResultStats


class ResultsBuilder:
    def __init__(self, tp=None, tn=None, fp=None, fn=None):
        self.tp = tp
        self.tn = tn
        self.fp = fp
        self.fn = fn

    def build(self):
        ret = []
        if self.tp is not None:
            for i in range(0,  self.tp):
                # (actual, predicted)
                ret.append((1, 1))
        if self.tn is not None:
            for i in range(0, self.tn):
                ret.append((0, 0))
        if self.fp is not None:
            for i in range(0, self.fp):
                ret.append((0, 1))
        if self.fn is not None:
            for i in range(0, self.fn):
                ret.append((1, 0))

        return ResultStats('', ret)


class TestResultStats(unittest.TestCase):
    def test_precision(self):
        self.assertEqual(ResultsBuilder(tp=2).build().get_precision(), 1)
        self.assertEqual(ResultsBuilder(tp=1, fp=1).build().get_precision(), 0.5)
        self.assertEqual(ResultsBuilder(fp=1).build().get_precision(), 0)

    def test_recall(self):
        self.assertEqual(ResultsBuilder(tp=1).build().get_recall(), 1)
        self.assertEqual(ResultsBuilder(tp=1, fn=1).build().get_recall(), 0.5)
        self.assertEqual(ResultsBuilder(fn=1).build().get_recall(), 0)

    def test_f1(self):
        recall = float(2)/(2+4)
        prec = float(2)/(2+3)
        f1 = 2*((float(recall)*prec)/(float(recall)+prec))
        self.assertEqual(ResultsBuilder(tp=2, fp=3, fn=4).build().get_f1_score(), f1)