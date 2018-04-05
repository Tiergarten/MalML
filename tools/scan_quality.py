from model_gen.mg_common import *
from datetime import datetime


def debug_samples(s_to_exec, label):
    total = UploadSearch().s().execute().hits.total
    print '{}: {} ({})'.format(
        label, s_to_exec.count(), float(s_to_exec.count())/total*100)


def get_sample_by_id(sha_id):
    return get_elastic().get(
        index=config.ES_CONF_SAMPLES[0], doc_type=config.ES_CONF_SAMPLES[1],
        id=sha_id)


def get_upload_by_id(sha_id):
    return UploadSearch(sample=sha_id).search()

def get_dt(dt_str):
    if '.' not in dt_str:
        return datetime.strptime(dt_str+'.0', '%Y-%m-%d %H:%M:%S.%f')
    else:
        return datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S.%f')


if __name__ == '__main__':
    good_s = UploadSearch().s().filter('range', **{'output-before-gz': {'gte': 2}})
    good_res = good_s[0:good_s.count()].execute()

    bad_s = UploadSearch().s().filter('match', status='ERR')
    bad_res = bad_s[0:bad_s.count()].execute()

    debug_samples(good_s, 'GOOD')
    debug_samples(bad_s, 'ERR')
    debug_samples(UploadSearch().s().filter('match', status='WARN'), 'WARN')

    for upload in good_res:
        sample_sha = upload.sample
        sample = get_sample_by_id(sample_sha)['_source']

        print 'GOOD sample info:{} -> {} [{} -> {}] {} {}'.format(
            sample_sha,
            sample['arch'],
            upload.exec_start_tm,
            upload.exec_end_tm,
            upload['output-before-gz'],
            (get_dt(upload.exec_end_tm) - get_dt(upload.exec_start_tm)))

    # FACK!
    for sample in bad_res:
        sample_sha = sample.sample
        print 'BAD sample info:{} -> {} ({})'.format(
            sample_sha, get_sample_by_id(sample_sha)['_source']['arch'],
            get_sample_by_id(sample_sha)['_source']['source'])
