from __future__ import division

import pandas as pd

class QueryParser(object):
    '''
    Simple parser to allow users to be a bit sloppy with thier queries and still
    get what they want.
    '''
    def __init__(self, prefix=u'ga:'):
        self.prefix = prefix
    
    def parse(self, **kwargs):
        '''
        Check query structure to ensure formatting is valid
        '''
        query = {}
        query.update(kwargs)
        
        # 1. Dates
        # The next two steps keep things consistent if the query is to be archived.
        try:
            if query['start_date'][-7:] == 'daysAgo':
                sd = pd.datetime.today() + \
                    pd.tseries.offsets.DateOffset(days=-int(query['start_date'][:-7]))
                query['start_date'] = sd.strftime('%Y-%m-%d')
                
            elif query['start_date'] == 'yesterday':
                sd = pd.datetime.today() + pd.tseries.offsets.DateOffset(days=-1)
                query['start_date'] = sd.strftime('%Y-%m-%d')
                
            elif query['start_date'] == 'today':
                query['start_date'] = pd.datetime.today().strftime('%Y-%m-%d')
                
            else:
                query['start_date'] = pd.to_datetime(query['start_date'], \
                    format='%Y-%m-%d').strftime('%Y-%m-%d')
                
        except (KeyError, AttributeError) as e:
            raise ValueError('The (required) \'start_date\' parameter is missing or invalid')
            
        try:
            if (query['end_date'] is None) | (query['end_date'] == 'today'):
                end_date = pd.datetime.today().strftime('%Y-%m-%d')
                
            else:
                query['end_date'] = pd.to_datetime(query['end_date']).strftime('%Y-%m-%d')
                
        except KeyError:
            query['end_date'] = pd.datetime.today().strftime('%Y-%m-%d')
        
        # 2. Prefixing
        # Ensure that all fields that should be in the form 'prefix:XXXX' acutally are.
        # Error handling skipped at this location intentionally.
        #
        # First sneak in fix to allow providing ids as int value
        query['ids'] = str(query['ids'])
        
        # this is a bit rough, but I don't want to put in a defualt
        # empty value anywhere as it would be in confilct with the rest
        # of the methodology
        try:
            names = 'ids', 'dimensions', 'metrics'
            lst = query['ids'], query['dimensions'], query['metrics']
            [self._maybe_add_arg(query, n, d) for n, d in zip(names, lst)]
            
        except KeyError as e:
            names = 'ids', 'metrics'
            lst = query['ids'], query['metrics']
            [self._maybe_add_arg(query, n, d) for n, d in zip(names, lst)]
        
        # 3. Clean up the filtering if present
        try:
            [self._maybe_add_filter_arg(query, n, d) \
                for n, d in zip(['filters'], [query['filters']])]
            
        except KeyError:
            pass
        
        # 4. sorting
        try:
            [self._maybe_add_sort_arg(query, n, d) \
                for n, d in zip(['sort'], [query['sort']])]
            
        except KeyError:
            pass
        
        # 5. start_index, max_results
        try:
            if query['start_index'] is not None:
                query['start_index'] = str(query['start_index'])
            
        except KeyError:
            pass
        
        try:
            if query['max_results'] is not None:
                query['max_results'] = str(query['max_results'])
            
        except KeyError:
            pass
        
        # 6. samplingLevel
        try:
            if query['samplingLevel'] is not None:
                query['samplingLevel'] = query['samplingLevel'].upper()
                
                if query['samplingLevel'].upper() not in ['DEFAULT', 'FASTER', 'HIGHER_PRECISION']:
                    query.pop('samplingLevel')
                    
                    print('Invalid value for \'samplingLevel\' specified, using \'DEFAULT\' instead')
                    
                    
        except KeyError:
            pass
        
        # 7. Remove options that should not be there
        for key in query.keys():
            if key not in ['ids', 'start_date', 'end_date', 'metrics', \
                            'dimensions', 'sort', 'filters', 'segment', \
                            'samplingLevel', 'start_index', 'max_results', \
                            'output', 'fields', 'userIp', 'quotaUser']:
                query.pop(key)
                
                print('Removed invalid query parameter \'{0}\''.format(key))
        
        # Nothing to do for 'segment' actually as it's too fleixible to
        # fix into this fix-via-intuition framework, at least I'm not seeing
        # anything obvious. For now, the user has to get it correct.
        #
        # TODO:
        # Add fixes for:
        # * fields
        # * userIp
        # * quotaUser
        
        return query

    def _maybe_add_arg(self, query, field, data):
        # Kludge to account for the fact that the same (GA) ids value is used
        # for different google products.
        if field == 'ids':
            prefix = 'ga:'
        else:
            prefix = self.prefix
            
        d = len(prefix)
            
        if data is not None:
            if isinstance(data, (pd.compat.string_types, int)):
                data = [data]
            data = ','.join(['{0}{1}'.format(prefix, x) if x[:d] != prefix \
                    else x for x in data])
            
            query[field] = data
            
    def _maybe_add_sort_arg(self, query, field, data):
        d = len(self.prefix)
        if data is not None:
            if isinstance(data, (pd.compat.string_types, int)):
                data = [data]
                
            def _prefix(item):
                if item[0] is '-':
                    if item[1:(d + 1)] == self.prefix:
                        return item
                        
                    else:
                        return self.prefix.join([item[0], item[1:]])
                    
                elif item[:d] != self.prefix:
                    return self.prefix + item
                
                else:
                    return item
                
            data = ','.join([_prefix(x) for x in data])
            
            query[field] = data
            
    def _maybe_add_filter_arg(self, query, field, data):
        d = len(self.prefix)
        if data is not None:
            if isinstance(data, (pd.compat.string_types, int)):
                data = [data]
                
            def _prefix(item):
                if item[:d] != self.prefix:
                    return self.prefix + item
                
                else:
                    return item
            
            if len(data) > 1:
                lookup = {'AND' : ';', 'OR' : ','}
                
                args = data[0::2]
                rules = [rule.upper() for rule in data[1::2]]
                
                if (len(set(rules).union(set(lookup.keys()))) > 2) | (len(args) - len(rules) != 1):
                    raise ValueError('Malformed / invalid filter' )
                
                res = ''.join([_prefix(arg) + rule for (arg, rule) in \
                    zip(args, [lookup[rule] for rule in rules])]) + _prefix(data[-1])
                
                query[field] = res
                
            else:
                self._maybe_add_arg(query, field, data)
