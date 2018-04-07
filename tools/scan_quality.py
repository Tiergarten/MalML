from model_gen.mg_common import *
from datetime import datetime


def debug_samples(s_to_exec, label, total):
    print '{}: {} ({})'.format(label, len(s_to_exec), float(len(s_to_exec))/total*100)

    breakdown =  breakdown_by_source(s_to_exec)
    for i in breakdown:
        print i, breakdown[i]

    print ""


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


def get_all_es(query):
    if query.count() > 0:
        return query[0:query.count()].execute()
    else:
        return []

def breakdown_by_source(input):
    counts = {}

    for upload in input:
        sample_sha = upload.sample
        sample = get_sample_by_id(sample_sha)['_source']

        if sample['source'] in counts.keys() :
            counts[sample['source']] += 1
        else:
            counts[sample['source']] = 1

    return counts

if __name__ == '__main__':
    total = UploadSearch().s().execute().hits.total

    good = get_all_es(UploadSearch().s().filter('match', status='OK'))
    bad = get_all_es(UploadSearch().s().filter('match', status='ERR'))
    warn = get_all_es(UploadSearch().s().filter('match', status='WARN'))

    good_w_upload = get_all_es(UploadSearch().s().filter('range', **{'output_before_gz': {'gte': 2}}))

    debug_samples(good, 'GOOD', total)
    debug_samples(bad, 'ERR', total)
    debug_samples(warn, 'WARN', total)

    debug_samples(good_w_upload, 'GOOD_W_UPLOAD', len(good))

    if False:
        for upload in good:
            sample_sha = upload.sample
            sample = get_sample_by_id(sample_sha)['_source']

            print 'GOOD sample info:{} -> {} [{} -> {}] {} {}'.format(
                sample_sha,
                sample['arch'],
                upload.exec_start_tm,
                upload.exec_end_tm,
                upload['output_before_gz'],
                (get_dt(upload.exec_end_tm) - get_dt(upload.exec_start_tm)))

        for sample in bad:
            sample_sha = sample.sample
            print 'BAD sample info:{} -> {} ({})'.format(
                sample_sha, get_sample_by_id(sample_sha)['_source']['arch'],
                get_sample_by_id(sample_sha)['_source']['source'])
