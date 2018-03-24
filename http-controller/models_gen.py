from common import *
import random
from model_gen.mg_common import *
import config


class UploadSearch:
    def __init__(self, extractor_pack=None, feature_family=None, sample=None):
        self._extractor_pack = extractor_pack
        self._feature_family = feature_family
        self._sample = sample

    @staticmethod
    def s():
        return Search(using=get_elastic(), index=config.REDIS_CONF_UPLOADS[0], doc_type=config.REDIS_CONF_UPLOADS[1])

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
    def __init__(self, label=None, arch=None, source=None):
        self._label = label
        self._arch = arch
        self._source = source

    @staticmethod
    def s():
        return Search(using=get_elastic(), index=config.REDIS_CONF_SAMPLES[0], doc_type=config.REDIS_CONF_SAMPLES[1])

    def search(self):
        ret = SampleSearch.s()
        if self._label is not None:
            ret = ret.filter('match', label=self._label)
        if self._arch is not None:
            ret = ret.filter('match', arch=self._arch)
        if self._source is not None:
            ret = ret.filter('match', source=self._source)

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


# TODO: source this from feature_extractors package
def get_feature_family_data(sample_upload, feature_fam):
    fn = os.path.join(config.FEATURES_DIR, sample_upload.sample, sample_upload.run_id,
                      '{}-0.0.1.json'.format(feature_fam))

    with open(fn, 'r') as fd:
        ret = json.loads(fd.read())

    return ret


def get_feature_sets_from_family(ffamily):
    return ffamily['feature_sets'].keys()


def generate_model(feature_fam):
    corpus_sz = 10
    metadata = {}

    corpus_ids = get_labelled_sample_set(feature_fam, 'benign', corpus_sz / 2) + \
        get_labelled_sample_set(feature_fam, 'malware', corpus_sz / 2)

    for sample in corpus_ids:
        print sample


if __name__ == '__main__':
    generate_model('ext-mem-rw-dump')
