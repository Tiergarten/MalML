import os
import config
from feature_extractors.fext_common import *
from common import *
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from scipy import stats, integrate
import seaborn as sns
sns.set(color_codes=True)

from common import *

if __name__ == '__main__':
    #np.set_printoptions(suppress=True,
  #                      formatter={'float_kind': '{:0.2f}'.format})

    chunk_sizes = ['1000', '2000', '5000']
    distributions = {
        '1000': np.array([]),
        '2000': np.array([]),
        '5000': np.array([])
    }

    cnt = 0
    for sample in os.listdir(config.FEATURES_DIR):

        if not is_sha256_fn(sample):
            continue

        ffam = FeatureFamily.from_file(os.path.join(config.FEATURES_DIR, sample, '0', 'ext-mem-rw-dump-0.0.1.json'))
        for feature_set in ffam.body['feature_sets']:
            feat_data = np.array(ffam.body['feature_sets'][feature_set]['feature_data'])
            for cs in chunk_sizes:
                if feature_set.endswith(cs):
                    distributions[cs] = np.concatenate((distributions[cs], feat_data))
        cnt += 1

    setup_logging('mem_rw_dump_dist.log')

    logging.info(str(cnt) + ' samples')
    for cs in chunk_sizes:
        dist = distributions[cs]

        logging.info(cs)
        logging.info('min: ' + str(dist.min().item()))
        logging.info('max: ' + str(dist.max().item()))
        logging.info(dist.shape)

        SAMPLES = 50
        SAMPLE_SZ = 150

        for i in range(0, SAMPLES):
            sample = np.random.choice(dist, SAMPLE_SZ, replace=False)
           #  sns.distplot(sample)
           # plt.xlabel('histogram {}'.format(cs))
           # plt.show()
           # plt.clf()

            sns.distplot(sample, hist=False, rug=True)
            logging.info('Created sample {}'.format(i))

        logging.info('done')
        sampled_perc = (float(SAMPLES*SAMPLE_SZ)/dist.shape[0])*100
        logging.info('sampled: {}%'.format(sampled_perc))

        plt.xlabel('chunk size: {}, {} KDE(s) (random sample size: {})'.format(cs, SAMPLES, SAMPLE_SZ))
        plt.show()
        plt.clf()

