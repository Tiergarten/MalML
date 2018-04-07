from model_gen.mg_common import *
from datetime import datetime


def debug_samples(s_to_exec, label, total):
    print '{}: {} ({})'.format(label, len(s_to_exec), float(len(s_to_exec))/total*100)

    breakdown =  samples_by_source_uniq(s_to_exec)
    for i in breakdown:
        print i, len(breakdown[i])

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


def samples_by_source_uniq(input):
    counts = {}

    for upload in input:
        sample_sha = upload.sample
        sample = get_sample_by_id(sample_sha)['_source']

        if sample['source'] in counts.keys():
            counts[sample['source']].add(sample_sha)
        else:
            counts[sample['source']] = set([sample_sha])

    return counts

if __name__ == '__main__':
    total = UploadSearch().s().execute().hits.total

    good = get_all_es(UploadSearch().s().filter('match', status='OK'))
    bad = get_all_es(UploadSearch().s().filter('match', status='ERR'))
    warn = get_all_es(UploadSearch().s().filter('match', status='WARN'))

    debug_samples(good, 'GOOD', total)
    debug_samples(bad, 'ERR', total)
    debug_samples(warn, 'WARN', total)


    for source in samples_by_source_uniq(good):
        cnt = 0
        for _sample in samples_by_source_uniq(good)[source]:
            result = FeatureSearch(sample=_sample).search()
            if len(result) > 0 and len(list(result.hits[0].feature_sets)) > 0:
                cnt += 1

        print source, cnt


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
