from models_gen import *

def debug_samples(s_to_exec, label):
    total = UploadSearch().s().execute().hits.total
    print '{}: {} ({})'.format(
        label, s_to_exec.count(), float(s_to_exec.count())/total*100)

def get_sample_by_id(sha_id):
    return get_elastic().get(
        index=config.REDIS_CONF_SAMPLES[0], doc_type=config.REDIS_CONF_SAMPLES[1],
        id=sha_id)

"""
good_res = good[0:good.count()].execute()

for sample in good_res.hits:
    sample_sha = sample.sample_url.split('/')[-1]
    print 'sample info: {}'.format(SampleSearch(sample=sample_sha).search())
"""

if __name__ == '__main__':
    debug_samples(UploadSearch().s().filter('range', **{'output-before-gz': {'gte': 2}}), 'GOOD')
    debug_samples(UploadSearch().s().filter('match', status='ERR'), 'ERR')
    debug_samples(UploadSearch().s().filter('match', status='WARN'), 'WARN')

    good_s = UploadSearch().s().filter('range', **{'output-before-gz': {'gte': 2}})
    good_res = good_s[0:good_s.count()].execute()

    for sample in good_res:
        sample_sha = sample.sample_url.split('/')[-1]
        print 'sample info:{} -> {}'.format(sample_sha,get_sample_by_id(sample_sha)['_source']['arch'])