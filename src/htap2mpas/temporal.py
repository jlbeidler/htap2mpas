import pandas as pd

class Temporal:

    def __init__(self, sector, rep_approach, log):
        '''
        Parameters

        sector: 
            the character string name of the HTAP sector used in the SCC field of the TREF

        rep_approach: 
            the character string identifying the representative day approach
             used for this sector, matching a column in the mrgdates file
             commonly used approaches for HTAP are aveday_N (one representative day per month),
             mwdss_N (four representative days per month), and ann_N (all days)
             The "_N" suffix designates that this approach ignores American holidays. 

        log:
            Python logging object
        '''
        self.sector = sector
        self.rep_approach = rep_approach
        self.log = log
        self.tref = None
        self.monthly = None
        self.weekly = None
        self.hourly = None
        self.dates = None

    def load_tref(self, fn):
        '''
        Load the TREF, assume that all temporal xref is at the sector level
        https://www.cmascenter.org/smoke/documentation/5.1/html/ch06s03s02.html
        '''
        self.log.info(f'Loading TREF:  {fn}')
        cols = ['sector','fips','fac','unit','relpt','proc','poll','dt_level','prof','cmt']
        dtype = dict((col, str) for col in cols)
        self.tref = pd.read_csv(fn, names=cols, comment='#', dtype=dtype, 
          usecols=['sector','dt_level','prof'])
        self.tref = self.tref[self.tref.sector == self.sector].drop_duplicates('dt_level')

    def load_monthly(self, fn):
        '''
        Load the monthly TPRO
        https://www.cmascenter.org/smoke/documentation/5.1/html/ch06s03.html#sect_input_tpro_monthly
        '''
        self.log.info(f'Loading TPRO_MONTHLY:  {fn}')
        usecols = ['prof',]+[str(n) for n in range(1,13)]
        self.monthly = pd.read_csv(fn, names=usecols+['cmt',], usecols=usecols, 
          dtype={'prof': str}, comment='#')
        self.monthly = pd.melt(self.monthly, id_vars='prof', var_name='month', value_name='mfrac')
        self.monthly = self._renorm(self.monthly, ['prof',], 'mfrac')
        self.monthly.month = self.monthly.month.astype(int)

    def load_weekly(self, fn):
        '''
        Load the day-of-week TPRO
        0-6 : Monday-Sunday
        https://www.cmascenter.org/smoke/documentation/5.1/html/ch06s03.html#sect_input_tpro_weekly
        '''
        self.log.info(f'Loading TPRO_WEEKLY:  {fn}')
        usecols = ['prof',]+[str(n) for n in range(7)]
        self.weekly = pd.read_csv(fn, names=usecols+['cmt',], usecols=usecols, 
          dtype={'prof': str}, comment='#')
        self.weekly = pd.melt(self.weekly, id_vars='prof', var_name='dow', value_name='wfrac')
        self.weekly = self._renorm(self.weekly, ['prof',], 'wfrac')
        self.weekly.dow = self.weekly.dow.astype(int)

    def load_hourly(self, fn):
        '''
        Load the diurnal profile
        0-23
        https://www.cmascenter.org/smoke/documentation/5.1/html/ch06s03.html#sect_input_tpro_hourly
        '''
        self.log.info(f'Loading TPRO_HOURLY:  {fn}')
        usecols = ['profile_id',]+[f'hour{n}' for n in range(1,25)]
        self.hourly = pd.read_csv(fn, usecols=usecols, dtype={'profile_id': str}, comment='#')
        newcols = ['prof',]+[n for n in range(24)]
        self.hourly.columns = newcols
        self.hourly = pd.melt(self.hourly, id_vars='prof', var_name='hour', value_name='hfrac')
        self.hourly = self._renorm(self.hourly, ['prof',], 'hfrac')

    def get_dates(self, fn):
        '''
        Read in the merge dates file that maps sequential days to representative days

        Header example:
        date,aveday_N,aveday_Y,mwdss_N,mwdss_Y,week_N,week_Y,all,all_N
        20160101,20160105,20160101,20160105,20160101,20160108,20160101,20160101,20160101
        '''
        df = pd.read_csv(fn, usecols=['date',self.rep_approach], 
          dtype={'date': str, self.rep_approach: str})
        df['dt'] = pd.to_datetime(df['date'])
        # Representative day datetime object
        df['repdt'] = pd.to_datetime(df[self.rep_approach])
        df['month'] = df['dt'].dt.month
        df['dow'] = df['dt'].dt.weekday
        self.dates = df.copy()

    def calc_month_to_hour(self):
        '''
        Calculate the month to hour temporal fractions as a df of representative datetime and hour
        '''
        for att in ['tref','weekly','hourly','dates']:
            if type(getattr(self, att)) == None:
                e = f'Must load {att} before temporal calculation'
                self.log.error(e)
                raise ValueError(e)
        w_prof = self.tref.loc[self.tref.dt_level == 'WEEKLY', 'prof'].values[0]
        month_to_day = self.weekly[self.weekly.prof == w_prof]
        h_prof = self.tref.loc[self.tref.dt_level == 'ALLDAY', 'prof'].values[0]
        day_to_hour = self.hourly[self.hourly.prof == h_prof]
        fracs = pd.merge(self.dates[['repdt','dow']].drop_duplicates('repdt'),
          month_to_day, on='dow', how='left')
        # Repeat day to hour by days in the fracs
        repdates = list(self.dates.repdt.drop_duplicates())
        day_to_hour = (pd.concat([day_to_hour]*len(repdates), 
          keys=repdates, names=['repdt',]).reset_index())
        fracs = fracs.merge(day_to_hour[['repdt','hour','hfrac']], on='repdt', how='left')
        # Month to hour fraction = ((month to week) * week to day) * day to hour
        fracs['frac'] = ((7/fracs.repdt.dt.days_in_month) * fracs.wfrac) * fracs.hfrac
        fracs = fracs[['repdt','hour','frac']].copy()
        fracs.rename(columns={'repdt': 'date'}, inplace=True)
        return fracs.sort_values(['date','hour'])

    def make_tz_aware(self, fracs, tzs):
        '''
        Parameters

        fracs:
            Hourly fraction dataframe of repdt, hour, frac
        tzs:
            List of timezone hourly offsets in input dataset

        Creates a new dataframe where the UTC date and hour are remapped to
          the local time temporal fraction based on an offset.
        A new column is added to indicate the offset.
        '''
        fracs_tz = pd.DataFrame()
        for tz in tzs:
            tz_dates = fracs[['date','hour']].copy()
            tz_dates['offset'] = tz
            tz_dates['ltime'] = tz_dates.date + pd.to_timedelta(tz_dates.hour + tz, unit='h') 
            fracs_tz = pd.concat((fracs_tz, tz_dates))
        # Remap the date-times to the TZ aware fractions
        # NOTE: This assumes that the input is monthly inventories rather than annual.
        #   Annual inventories allow shifting back to a previous month.
        # Now mappings are holiday aware
        fracs_tz['lhour'] = fracs_tz.ltime.dt.hour
        if self.rep_approach.startswith('aveday'):
            # Only one average day in a month, simply change the hours
            fracs_tz['ldate'] = fracs_tz['date']
        elif self.rep_approach.startswith('all'):
            # Using all days of the year, remap the hour
            fracs_tz['ldate'] = tz.dates.ltime.dt.date
        elif self.rep_approach.startswith('week'):
            # Remap to the day of week for the month 
            fracs_tz['dow'] = fracs_tz.ltime.dt.weekday
            # If shift to holiday, flag and replace ldate with holiday
            fracs_tz['month'] = fracs_tz.date.dt.month
            # If using annual fracs_tz['month'] = fracs_tz.ltime.dt.month
            dow_map = fracs[['date',]].drop_duplicates()
            dow_map['dow'] = dow_map['date'].dt.weekday
            dow_map['month'] = dow_map['date'].dt.month
            dow_map.rename(columns={'date': 'ldate'}, inplace=True)
            fracs_tz = fracs_tz.merge(dow_map, on=['dow','month'], how='left')
        elif self.rep_approach.startswith('mwdss'):
            # Remap to the day of week for the month 
            fracs_tz['dow'] = fracs_tz.ltime.dt.weekday
            # Map all non-Monday weekdays to Tuesday
            idx = fracs_tz.dow.isin(range(1,5))
            fracs_tz.loc[idx, 'dow'] = 1
            fracs_tz['month'] = fracs_tz.date.dt.month
            dow_map = fracs[['date',]].drop_duplicates()
            dow_map['dow'] = dow_map['date'].dt.weekday
            dow_map['month'] = dow_map['date'].dt.month
            dow_map.rename(columns={'date': 'ldate'}, inplace=True)
            fracs_tz = fracs_tz.merge(dow_map, on=['dow','month'], how='left')
        else:
            self.log.error(f'Invalid rep_approach:  {self.rep_approach}')
        fracs.rename(columns={'date': 'ldate', 'hour': 'lhour'}, inplace=True)
        fracs_tz = fracs_tz.merge(fracs, on=['ldate','lhour'], suffixes=['_orig',''])
        cols = ['date','hour','offset','frac']
        return fracs_tz[cols].copy()

    def _renorm(self, df, idx, val):
        '''
        Renormalize the temporal profile
        df - Dataframe to renormalize
        idx - list of id columns
        val = single value column
        '''
        cols = list(df.columns)
        sdf = df[idx+[val,]].groupby(idx, as_index=False).sum()
        df = df.merge(sdf, on=idx, how='left', suffixes=['','_sum'])
        df[val] = (df[val]/df[f'{val}_sum']).round(8)
        return df[cols].copy()
