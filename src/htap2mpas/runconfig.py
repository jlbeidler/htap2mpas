import json

class RunConfig:

    def __init__(self, config, log):
        '''
        Parameters

        config:
            Path to configuration JSON file

        log:
            Python logging object
        '''
        self._load_config(config)
        log.info(f'Loading configuration file {config}')
        self._set_defaults()
        # Load temporal
        temp_vars = ['mrgdates','rep_approach','tref','tpro_monthly','tpro_hourly','tpro_weekly']
        for v in temp_vars:
            setattr(self, v, self._get_config_option('temporal', v))
        # MPAS related
        mpas_vars = ['mpasref','gridmap','mesh']
        for v in mpas_vars:
            setattr(self, v, self._get_config_option('mpas', v))
        # HTAP related
        htap_vars = ['htapsector','layers','tz_mask']
        for v in htap_vars:
            setattr(self, v, self._get_config_option('htap', v))
        # Speciation
        spec_vars = ['invtable','gsref','gspro','mech']
        for v in spec_vars:
            setattr(self, v, self._get_config_option('speciation', v))
        # Top-level
        top_vars = ['sector','year','invlist','case']
        for v in top_vars:
            setattr(self, v, self._get_config_option(v))
        atts = top_vars + mpas_vars + htap_vars + temp_vars + spec_vars
        for att in atts:
            log.info(f'Config: {att} = {getattr(self, att)}')         

    def _load_config(self, config):
        '''
        '''
        with open(config) as f:
            self._config = json.load(f)

    def _set_defaults(self):
        '''
        '''
        self.defaults = {'layers': '',
                         'tz_mask': ''}

    def _get_config_option(self, l1, l2=False):
        '''
        '''
        try:
            val = self._config[l1]
        except KeyError as e:
            if l1 in self.defaults:
                val = self.defaults[l1]
                print(f'WARNING: Setting {l1} to default value {val}', flush=True)
            else:
                raise KeyError(f'Missing top-level configuration for {l1}')
        if l2:
            try:
                val = self._config[l1][l2]
            except KeyError as e:
                if l2 in self.defaults:
                    val = self.defaults[l2]
                    print(f'WARNING: Setting {l2} to default value {val}', flush=True)
                else:
                    raise KeyError(f'Missing configuration for {l1}:{l2}')
        return val

