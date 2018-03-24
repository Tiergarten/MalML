import importlib

from common import *
import config

from feature_extractors import fext_common

FEXT_MAP = {'pack-1': [('fext_mem_rw_dump', 'FextMemRwDump')]}


def get_fexts_for_pack(pack_nm):
    return FEXT_MAP[pack_nm]


def get_feature_extractor_for_pack(module, clazz):
    return getattr(importlib.import_module('feature_extractors.{}'.format(module)), clazz)


# TODO: This is slow, add logging for times...
if __name__ == '__main__':

    print 'beep'

    uploads = get_detonator_uploads(config.UPLOADS_DIR)
    print 'found {} uploads'.format(len(uploads)
                                    )
    for upload in get_detonator_uploads(config.UPLOADS_DIR):
        print 'processing {}'.format(upload)
        for run in upload.run_ids:

            if not upload.isSuccess():
                print 'skipping {} / {}'.format(upload.sample, run)
                continue

            metadata = DetonationMetadata(upload)

            for (module, clazz) in get_fexts_for_pack(metadata.get_extractor()):

                feature_ext_class = get_feature_extractor_for_pack(module, clazz)

                feature_writer = fext_common.FeatureSetsWriter(config.FEATURES_DIR, upload.sample, run,
                                                               feature_ext_class.extractor_name,
                                                               feature_ext_class.extractor_ver)

                feature_ext_class(feature_writer).run(upload.get_output(run))
