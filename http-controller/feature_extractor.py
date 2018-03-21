import os
import json
from common import *
import config
from feature_extractors import fext_common
from feature_extractors import fext_mem_rw_dump


if __name__ == '__main__':
    for upload in get_detonator_uploads(config.UPLOADS_DIR):
        print upload
        for run in upload.run_ids:
            feature_writer = fext_common.FeatureSetsWriter(upload.sample, run,
                                                           fext_mem_rw_dump.FextMemRwDump.extractor_name,
                                                           fext_mem_rw_dump.FextMemRwDump.extractor_ver)
            feature_extractor = fext_mem_rw_dump.FextMemRwDump(feature_writer)
            feature_extractor.run(upload.get_output(run))
