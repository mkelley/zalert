# Licensed under a 3-clause BSD style license - see LICENSE.rst
#import re
import os
import sqlite3

import numpy as np
import astropy.units as u
from astropy.time import Time
from astropy.table import Table, Column
from astropy.coordinates import Angle
import fastavro
from sbsearch import SBSearch

from . import schema, util
from .config import Config
from sbsearch.logging import ProgressTriangle

HALF_SIZE = 30 / 206265  # assumed half-FOV size, radians

class ZAlert(SBSearch):
    """ZTF alert checker for small bodies.

    Parameters
    ----------
    config : Config, optional
        ZAlert configuration class, or ``None`` to load the default.

    save_log : bool, optional
        Set to ``True`` to log to file.

    disable_log : bool, optional
        Set to ``True`` to disable normal logging; also sets
        ``save_log=False``.

    **kwargs
        If ``config`` is ``None``, pass these additional keyword
        arguments to ``Config`` initialization.

    """

    def __init__(self, config=None, save_log=False, disable_log=False,
                 **kwargs):
        self.config = Config(**kwargs) if config is None else config
        super().__init__(config=config, save_log=save_log,
                         disable_log=disable_log, **kwargs)
        
    def summarize_found(self, objects=None, start=None, stop=None):
        """Summarize found object database."""
        kwargs = {
            'objects': objects,
            'start': start,
            'stop': stop,
            'columns': ('foundid,candid,desg,jd,ra,dec,ra3sig,dec3sig,'
                        'vmag,fid,rh,rdot,delta,phase,selong'),
            'inner_join': ['alerts USING (candid)']
        }
        tab = super().summarize_found(**kwargs)

        if tab is None:
            return None

        date = [d[:16] for d in Time(tab['jd'], format='jd').iso]
        tab.add_column(Column(date, name='date'), 2)
        tab['ra'] = Angle(tab['ra'], 'deg').to_string(
            sep=':', precision=1, pad=True, unit='hourangle')
        tab['dec'] = Angle(tab['dec'], 'deg').to_string(
            sep=':', precision=0, pad=True)
        tab['rh'] = tab['rh'] * np.sign(tab['rdot'])
        tab['fid'].name = 'filt'
        tab.remove_column('jd')
        tab.remove_column('rdot')

        for col in ('ra3sig', 'dec3sig', 'phase', 'selong'):
            tab[col].format = '{:.0f}'
        tab['vmag'].format = '{:.1f}'
        for col in ('rh', 'delta'):
            tab[col].format = '{:.2f}'

        return tab

    def update_alerts(self, source_path):
        """Ingest AVRO alerts from a directory.

        Parameters
        ----------
        source_path : string
            Location of the alerts.

        """

        read = 0
        added = 0
        errored = 0
        now = Time.now().iso

        alerts_insert = '''
        INSERT INTO alerts VALUES (
          :nightid,last_insert_rowid(),:jd,:fid,:pid,:diffmaglim,
          :pdiffimfilename,:programpi,:programid,:candid,
          :isdiffpos,:tblid,:nid,:rcid,:field,:xpos,:ypos,
          :ra,:dec,:magpsf,:sigmapsf,:chipsf,:magap,
          :sigmagap,:distnr,:magnr,:sigmagnr,:chinr,
          :sharpnr,:sky,:magdiff,:fwhm,:classtar,
          :mindtoedge,:magfromlim,:seeratio,:aimage,:bimage,
          :aimagerat,:bimagerat,:elong,:nneg,:nbad,:rb,
          :ssdistnr,:ssmagnr,:ssnamenr,:sumrat,:magapbig,
          :sigmagapbig,:ranr,:decnr,:sgmag1,:srmag1,:simag1,
          :szmag1,:sgscore1,:distpsnr1,:ndethist,:ncovhist,
          :jdstarthist,:jdendhist,:scorr,:tooflag,
          :objectidps1,:objectidps2,:sgmag2,:srmag2,:simag2,
          :szmag2,:sgscore2,:distpsnr2,:objectidps3,
          :sgmag3,:srmag3,:simag3,:szmag3,:sgscore3,
          :distpsnr3,:nmtchps,:rfid,:jdstartref,:jdendref,
          :nframesref,:exptime)
        '''

        tri = ProgressTriangle(1, self.logger, base=10)
        for path, dirs, files in os.walk(source_path):
            for f in files:
                if not f.endswith('avro'):
                    continue

                read += 1
                tri.update()
                alert = util.avro2dict(os.path.join(path, f))
                candidate = alert['candidate']

                night = Time(candidate['jd'], format='jd').iso[:10]

                # exptime does not exist in older alerts
                jd0 = candidate['jd']
                jd1 = jd0 + candidate.get('exptime', 0)
                candidate['exptime'] = candidate.get('exptime')

                try:
                    c = self.db.cursor()
                    c.execute('''
                    INSERT OR IGNORE INTO nights
                    VALUES (NULL,:date,0,:now)
                    ''', dict(date=night, now=now))

                    candidate['nightid'] = c.execute('''
                    SELECT nightid FROM nights
                       WHERE date=:date
                    ''', dict(date=night)).fetchone()[0]

                    ra = np.radians(candidate['ra'])
                    dec = np.radians(candidate['dec'])
                    points = util.define_points(ra, dec, HALF_SIZE)
                    rows = [[None, 'alerts', jd0, jd1, points]]
                    self.db.add_observations(
                        rows, other_cmd=alerts_insert,
                        other_rows=[candidate])
                except sqlite3.IntegrityError:
                    errored += 1
                    continue

                c.execute('''
                UPDATE nights SET alerts=alerts+1 WHERE date=:date
                ''', dict(date=night))
                
                added += 1

        self.logger.info('{} files read, {} added to database.'
                         .format(read, added))
        self.logger.warning('{} errored transactions.'.format(errored))

    def verify_database(self):
        """Verify database tables, triggers, etc."""
        super().verify_database(names=schema.zalert_names,
                                script=schema.schema)
