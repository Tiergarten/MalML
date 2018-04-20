import os
import config
from feature_extractors.fext_common import *
from common import *
import numpy as np
import seaborn as sns
sns.set(color_codes=True)

if __name__ == '__main__':

    distributions = {
        '1000': np.array([]),
        '2000': np.array([]),
        '5000': np.array([])
    }

    for sample in os.listdir(config.FEATURES_DIR):
        if not is_sha256_fn(sample):
            continue

        ffam = FeatureFamily.from_file(os.path.join(config.FEATURES_DIR, sample, '0', 'ext-mem-rw-dump-0.0.1.json'))
        for feature_set in ffam.body['feature_sets']:
            if feature_set.endswith("-1000"):
                print ffam.body['feature_sets'][feature_set]
                feat_data = np.array(ffam.body['feature_sets'][feature_set]['feature_data'])
                distributions['1000'] = np.concatenate((distributions['1000'], feat_data))


    print distributions['1000']
    sns.distplot(distributions['1000'], hist=False, rug=True)
