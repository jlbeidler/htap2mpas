import pandas as pd

class Speciation:

    def __init__(self, sector, log):
        '''
        Parameters

        sector: 
            the character string name of the HTAP sector used in the SCC field of the TREF

        log:
            Python logging object
        '''
        self.sector = sector
        self.log = log
        self.gsref = None
        self.gspro = None
        self.polls = []

    def load_gsref(self, fn):
        '''
        Load the GSREF, assume that all speciation xref is at the sector level
        https://www.cmascenter.org/smoke/documentation/5.1/html/ch06s05s05.html#d0e35907
        '''
        self.log.info(f'Loading GSREF:  {fn}')
        cols = ['sector','prof','poll','fips','matc','sic','fac','unit','relpt','proc']
        dtype = dict((col, str) for col in cols)
        self.gsref = pd.read_csv(fn, names=cols, comment='#', dtype=dtype, sep=';',
          usecols=['sector','poll','prof'], skip_blank_lines=True)
        # Basic hierarchy either sector-specific or for all sectors
        idx = self.gsref.sector.fillna('').str.strip() == ''
        self.gsref.loc[idx, 'sector'] = '0' 
        self.gsref = self.gsref[(self.gsref.sector == self.sector) | (self.gsref.sector.str.startswith('0'))].copy()
        self.gsref = self.gsref.sort_values(['poll','sector']).drop_duplicates('poll', keep='last')

    def load_gspro(self, fn):
        '''
        Load the GSPRO
        https://www.cmascenter.org/smoke/documentation/5.1/html/ch06s05s02.html
        '''
        self.log.info(f'Loading GSPRO:  {fn}')
        names = ['prof','poll','spec','frac','mw','massfrac']
        self.gspro = pd.read_csv(fn, names=names, dtype={'prof': str, 'poll': str}, 
          usecols=['prof','poll','spec','frac','mw'], sep=';', comment='#')

    def get_spec_table(self):
        '''
        Return the speciation tables for the sector 
        This does not include any TOG conversions with the gscnv
        '''
        spec_table = self.gsref.merge(self.gspro, on=['poll','prof'], how='left')
        spec_table = spec_table[spec_table.frac.notnull()].copy()
        polls = list(self.gsref.poll.drop_duplicates())
        # Gapfill missing pollutants with default profiles
        def_prof = self.gspro[self.gspro.prof.fillna('0').str.strip().str.startswith('0')].copy()
        def_prof = def_prof[~ def_prof.poll.isin(polls)].copy()
        spec_table = pd.concat((spec_table, def_prof))
        if len(spec_table[spec_table.duplicated(['poll','spec'])]) > 0:
            e = f'Duplicate speciation profiles detected for the sector'
            self.log.error(e)
            raise ValueError(e)
        self.polls = list(self.gsref.poll.drop_duplicates())
        self.log.debug(f'Creating speciation profiles for "{",".join(self.polls)}"')
        return spec_table 
