import re
import unittest
import pandas as pd
import numpy as np
from fext_common import *
from enum import Enum
import pdb
import sys
import logging
import gc


class FextMemRwDump:
    extractor_name = 'ext-mem-rw-dump'
    __version__ = '0.0.1'

    def __init__(self, feature_set_writer, worker_id):
        self.feature_set_writer = feature_set_writer
        self.logger = logging.getLogger('{}-{}-{}'.format(self.__class__.__name__,
                                                       feature_set_writer.body['sample_id'][0:7],worker_id))


    @staticmethod
    def parse_line(str, type):
        regex = r'(\w+): (W|R) (\w+) +(\d) +(\w+)'
        match = re.match(regex, str)

        if match:
            ins_addr = match.group(1)
            rw = match.group(2)
            tgt_addr = match.group(3)

            if rw in type.upper():
                return int(ins_addr, 16), rw, int(tgt_addr, 16)

        return None

    @staticmethod
    def get_df(stats):
        idx = [s[0] for s in stats]
        mem_access = {
            'RW': pd.Series([s[1] for s in stats], index=idx),
            'TGT': pd.Series([s[2] for s in stats], index=idx)
        }

        df = pd.DataFrame(mem_access)
        return df

    @staticmethod
    def get_df_from_lines(lines, type='rw'):
        stats = []
        for line in lines:
            p = FextMemRwDump.parse_line(line, type)
            if p is not None:
                stats.append(p)

        return FextMemRwDump.get_df(stats)

    @staticmethod
    def get_idx_val(row):
        return row.index.values[0]

    @staticmethod
    def get_val(pdtype):
        assert len(pdtype.values) == 1
        return int(pdtype.values[0])

    class ChunkMode(Enum):
        DEFAULT = 1
        SEQUENTIAL_BY_OCCURRENCE = 1
        BY_SOURCE_ADDR = 2  # TODO

    # TODO: Should create lots of different versions of this to compare accuracy...
    @staticmethod
    def get_chunks(df, chunk_sz_instr, chunk_mode=ChunkMode.DEFAULT):

        if len(df) < chunk_sz_instr:
            return [df]

        if chunk_mode == FextMemRwDump.ChunkMode.SEQUENTIAL_BY_OCCURRENCE:
            return FextMemRwDump.get_chunks_seq_by_occurrence(df, chunk_sz_instr)
        elif chunk_mode == FextMemRwDump.ChunkMode.BY_SOURCE_ADDR:
            return FextMemRwDump.get_chunks_seq_by_src_addr(df, chunk_sz_instr)

    @staticmethod
    def get_chunks_seq_by_occurrence(df, chunk_sz_instr):

        n_chunks = len(df) / chunk_sz_instr

        ret = []
        for i in range(0, n_chunks + 1):
            start_idx = (i * chunk_sz_instr)
            chunk_df = df.iloc[start_idx:start_idx + chunk_sz_instr]
            if not chunk_df.empty:
                ret.append(chunk_df)

        return ret

    @staticmethod
    def get_chunks_seq_by_src_addr(df, chunk_sz_instr):
        pass


    class MemOffsetMode(Enum):
        DEFAULT = 1
        SUM_ABS_REF = 1
        SUM_REF_ASC = 2
        MAX_REF = 3
        MIN_REF = 4
        MEAN = 5

    @staticmethod
    def get_chunk_mem_deltas(df, chunk_sz_instr=10000, mode=MemOffsetMode.DEFAULT):
        ret = []
        for c in FextMemRwDump.get_chunks(df, chunk_sz_instr):
            # TODO: do we want to divide these results by chunk sz??
            # TODO: what about the size of the binary also
            ret.append(FextMemRwDump.calc_mem_access_delta(c, mode))

        return ret

    @staticmethod
    def calc_mem_access_delta(df, mode=MemOffsetMode.DEFAULT):
        ret = 0

        if len(df) == 1:
            return 0

        base_reference = df.head(1)['TGT'].values[0]
        tgt_deltas = df['TGT'].apply(lambda mem_access: mem_access - base_reference)

        if mode == FextMemRwDump.MemOffsetMode.SUM_ABS_REF:
            ret = abs(tgt_deltas.sum())
        elif mode == FextMemRwDump.MemOffsetMode.SUM_REF_ASC:
            ret = tgt_deltas.sum()
        elif mode == FextMemRwDump.MemOffsetMode.MAX_REF:
            ret = tgt_deltas.max()
        elif mode == FextMemRwDump.MemOffsetMode.MIN_REF:
            ret = tgt_deltas.min()
        elif mode == FextMemRwDump.MemOffsetMode.MEAN:
            ret = tgt_deltas.mean()

        return ret

    @staticmethod
    def get_histogram(instr_chunk_sz, chunk_deltas, feature_name, feature_set_writer):

        assert len(chunk_deltas) > 2, "Not enough chunks for histogram!"

        tdeltas = pd.DataFrame(chunk_deltas)

        # Emit chunk metadata (min,max), so we can see if range should expand as we process BAU
        minmax_metadata = {
                              'histogram_min': FextMemRwDump.get_val(tdeltas.min()),
                              'histogram_max': FextMemRwDump.get_val(tdeltas.max()),
                              'histogram_mean': FextMemRwDump.get_val(tdeltas.mean()),
                              'histogram_std_dev': FextMemRwDump.get_val(tdeltas.std())
        }

        feature_set_writer.write_metadata(feature_name, minmax_metadata)

        histogram_ranges = {
            '1000': (-318537000000000, 365617000000000),
            '2000': (-18397000000000, 17959000000000),
            '5000': (-343961000000000, 359544000000000)
        }

        return np.histogram(tdeltas, range=histogram_ranges[str(instr_chunk_sz)], bins=1024)

    def extract_feature_set(self, df, access_type, instr_chunk_sz, feature_set, debug_distribution):
        feature_name = "%s-%s-%s" % (access_type, feature_set, instr_chunk_sz)
        self.logger.info('extracting feature: {}'.format(feature_name))
        sys.stdout.flush()

        # Split mem access into chunks, and calculate the mem access delta (from first in chunk)
        chunk_tgt_deltas = FextMemRwDump.get_chunk_mem_deltas(df, instr_chunk_sz, feature_set)

        if debug_distribution:
            self.feature_set_writer.write_feature_set(feature_name, [x.item() for x in chunk_tgt_deltas])
            return

        # Produce histogram
        if len(chunk_tgt_deltas) < 3:
            self.logger.warn('not enough chunks for histogram, skipping {}'.format(feature_name))
            return

        hist, bin_edges = FextMemRwDump.get_histogram(instr_chunk_sz, chunk_tgt_deltas, feature_name,
                                                      self.feature_set_writer)
        # TODO: We don't want to write scientific notation
        self.feature_set_writer.write_feature_set(feature_name, hist.tolist())

    def run(self, fn, debug=False):
        pd.set_option('display.float_format', lambda x: '%.2f' % x)
        np.set_printoptions(suppress=True)

        from_file = get_pintool_output(fn)
        if len(from_file) == 0:
            self.logger.warn('no data in {}, skipping'.format(fn))
            return

        self.logger.info('parsing {}'.format(fn))

        for access_type in ['R', 'W', 'RW']:
            df = FextMemRwDump.get_df_from_lines(from_file, access_type)

            for instr_chunk_sz in [1000, 2000, 5000]:
                for feature_set in FextMemRwDump.MemOffsetMode:

                    if feature_set == FextMemRwDump.MemOffsetMode.DEFAULT:
                        continue

                    if len(df) < instr_chunk_sz:
                        self.logger.warn('not enough instructions to fill a chunk, skipping...')
                        continue

                    self.extract_feature_set(df, access_type, instr_chunk_sz, feature_set, debug)

        self.feature_set_writer.write_feature_sets()
        self.logger.info('finished')
