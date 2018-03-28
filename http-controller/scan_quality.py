from models_gen import *

good = UploadSearch().s().filter('range', **{'output-before-gz': {'gte': 2}})
total = UploadSearch().s().execute()

print 'got {}/{} ({}%) good uploads'.format(good.count(), total.hits.total, float(good.count())/total.hits.total*100)

good_res = good[0:good.count()].execute()

for sample in good_res.hits:
	sample_sha = sample.sample_url.split('/')[-1]
	print 'sample info: {}'.format(SampleSearch(sample=sample_sha).search())
