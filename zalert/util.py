# Licensed under a 3-clause BSD style license - see LICENSE.rst

import numpy as np
import fastavro
from sbsearch import util

def define_points(ra, dec, half_size):
    """Points for SBSearch.add_observations.

    ra, dec, half_size in radians

    Returns: [ra_center, dec_center, ra1, dec1, ra2, dec2, ra3, dec3,
    ra4, dec4]
    """
    d = dec + np.r_[1, 1, -1, -1] * half_size
    r = ra + np.r_[1, -1, -1, 1] * half_size * np.cos(d)
    points = [ra, dec, r[0], d[0], r[1], d[1], r[2], d[2], r[3], d[3]]
    return points

def avro2dict(filename):
    '''read avro data into dictionary'''
    with open(filename, 'rb') as inf:
        reader = fastavro.reader(inf)
        candidate = next(reader, None)
    return candidate
