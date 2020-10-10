import pandas as pd

import re

class QueryParser(object):
    '''
    Simple parser to allow users to be a bit sloppy with thier queries and still
    get what they want.
    '''
    def __init__(self, prefix='ga:'):
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
            if query.get('start_date', '').endswith('daysAgo'):
                ndays = int(re.sub(r'daysAgo', '', query.get('start_date')))
                sday = pd.Timestamp('today') + pd.Timedelta(days=ndays)

                query.update({
                    'start_date' : sday.strftime('%Y-%m-%d')
                })

            elif query.get('start_date', '') == 'yesterday':
                sday = pd.Timestamp('today') + pd.Timedelta(days=-1)

                query.update({
                    'start_date' : sday.strftime('%Y-%m-%d')
                })

            elif query.get('start_date', '') == 'today':
                query.update({
                    'start_date' : pd.Timestamp('today').strftime('%Y-%m-%d')
                })
                
            else:
                # force the formatting to a string YYYY-mm-dd
                sday = pd.Timestamp(query.get('start_date')).strftime('%Y-%m-%d')
                query.update({
                    'start_date' : sday
                })

        except (KeyError, AttributeError) as e:
            raise ValueError('The (required) \'start_date\' parameter is missing or invalid')

        if (query.get('end_date') is None) | (query.get('end_date') == 'today'):
            query.update({
                'end_date' : pd.Timestamp('today').strftime('%Y-%m-%d')
            })

        else:
            eday = pd.Timestamp(query.get('end_date')).strftime('%Y-%m-%d')
            query.update({
                'end_date' : eday
            })

        # 2. Prefixing
        # Ensure that all fields that should be in the form 'prefix:XXXX' acutally are.
        # Error handling skipped at this location intentionally.
        #
        # First sneak in fix to allow providing ids as int value
        query.update({
            'ids' : str(query.get('ids', ''))
        })
        
        # this is a bit rough, but I don't want to put in a defualt
        # empty value anywhere as it would be in confilct with the rest
        # of the methodology
        try:
            names = ['ids', 'dimensions', 'metrics']
            lst = [query.get(e) for e in names]
            _ = [self._maybe_add_arg(query, n, d) for n, d in zip(names, lst)]
            
        except KeyError as e:
            names = ['ids', 'metrics']
            lst = [query.get(e) for e in names]
            _ = [self._maybe_add_arg(query, n, d) for n, d in zip(names, lst)]
        
        # 3. Clean up the filtering if present
        _ = [self._maybe_add_filter_arg(query, n, d) \
            for n, d in zip(['filters'], [query.get('filters', '')])]

        # 4. sorting
        _ = [self._maybe_add_sort_arg(query, n, d) \
                for n, d in zip(['sort'], [query.get('sort', '')])]

        # 5. start_index, max_results
        if query.get('start_index') is not None:
            query.update({
                'start_index' : str(query.get('start_index'))
            })

        if query.get('max_results') is not None:
            query.update({
                'max_results' : str(query.get('max_results'))
            })
        
        # 6. samplingLevel
        if query.get('samplingLevel') is not None:
            lvl = query.get('samplingLevel').upper()

            if lvl not in ['DEFAULT', 'FASTER', 'HIGHER_PRECISION']:
                print('Invalid value for \'samplingLevel\' specified, using \'DEFAULT\' instead')

                lvl = 'DEFAULT'

            query.update({
                'samplingLevel' : lvl
            })

        # 7. Remove options that should not be there
        valid_params =  {
            'ids',
            'start_date',
            'end_date',
            'metrics',
            'dimensions',
            'sort',
            'filters',
            'segment',
            'samplingLevel',
            'start_index',
            'max_results',
            'output',
            'fields',
            'userIp',
            'quotaUser'}

        temp = set(query).difference(valid_params)

        if any(temp):
            for key in temp:
                _ = query.pop(key)

                print(f'Removed invalid query parameter \'{key}\'')

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
            if isinstance(data, (str, int)):
                data = [data]
            data = ','.join(
                    [f'{prefix}{x}' if x[:d] != prefix else x for x in data]
                )
            
            query[field] = data
            
    def _maybe_add_sort_arg(self, query, field, data):
        d = len(self.prefix)
        if data is not None:
            if isinstance(data, (str, int)):
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
            if isinstance(data, (str, int)):
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
