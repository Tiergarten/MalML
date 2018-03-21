import re
import unittest
import pandas as pd
import numpy as np
from fext_common import *
from enum import Enum
import pdb


class FextMemRwDump:
    extractor_name = 'ext-mem-rw-dump'
    extractor_ver = '0.0.1'

    def __init__(self, feature_set_writer):
        self.feature_set_writer = feature_set_writer

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
    def get_df_from_file(lines, type='rw'):
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
        if chunk_mode == FextMemRwDump.ChunkMode.SEQUENTIAL_BY_OCCURRENCE:
            return FextMemRwDump.get_chunks_seq_by_occurrence(df, chunk_sz_instr)

    @staticmethod
    def get_chunks_seq_by_occurrence(df, chunk_sz_instr):

        if len(df) < chunk_sz_instr:
            return [df]

        n_chunks = len(df) / chunk_sz_instr

        ret = []
        for i in range(0, n_chunks + 1):
            start_idx = (i * chunk_sz_instr)
            chunk_df = df.iloc[start_idx:start_idx + chunk_sz_instr]
            if not chunk_df.empty:
                ret.append(chunk_df)

        return ret

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
    def get_histogram(chunk_deltas, feature_name, feature_set_writer):

        assert len(chunk_deltas) > 2, "Not enough chunks for histogram!"

        tdeltas = pd.DataFrame(chunk_deltas)

        # Emit chunk metadata (min,max), so we can see if range should expand as we process BAU
        minmax_metadata = {'histogram_min': FextMemRwDump.get_val(tdeltas.min()), 'histogram_max': FextMemRwDump.get_val(tdeltas.max())}
        feature_set_writer.write_metadata(feature_name, minmax_metadata)  # TODO: use of str() is broken here...

        # TODO: We will need to set static 'range' here so results are comparable across all binaries
        return np.histogram(tdeltas)

    def run(self, fn):
        pd.set_option('display.float_format', lambda x: '%.2f' % x)
        np.set_printoptions(suppress=True)

        for access_type in ['R', 'W', 'RW']:
            df = FextMemRwDump.get_df_from_file(get_pintool_output(fn), access_type)

            for instr_chunk_sz in [1000, 5000, 10000, 25000]:
                for mode in FextMemRwDump.MemOffsetMode:

                    if mode == FextMemRwDump.MemOffsetMode.DEFAULT:
                        continue

                    if len(df) < instr_chunk_sz:
                        print 'not enough instructions to fill a chunk, skipping...'
                        continue

                    feature_name = "%s-%s-%s" % (access_type, mode, instr_chunk_sz)

                    # Split mem access into chunks, and calculate the mem access delta (from first in chunk)
                    chunk_tgt_deltas = FextMemRwDump.get_chunk_mem_deltas(df, instr_chunk_sz, mode)
                    print 'chunk deltas: %s' % chunk_tgt_deltas

                    # Produce histogram
                    divisions, counts = FextMemRwDump.get_histogram(chunk_tgt_deltas, feature_name, self.feature_set_writer)
                    # TODO: We don't want to write scientific notation
                    self.feature_set_writer.write_feature_set(feature_name, counts.tolist())

                    print "a: %s" % (type(counts.tolist()[0]))
                    print 'histogram buckets: %s' % divisions
                    print 'histogram counts: %s' % counts
                    print '--'

        self.feature_set_writer.write_feature_sets()

if __name__ == '__main__':
    feature_set_writer = FeatureSetsWriter("traceme.exe", 0, EXTRACTOR_NAME, __version__)
    fmrd = FextMemRwDump(feature_set_writer)
    fmrd.run('aext-mem-rw-dump.out')
