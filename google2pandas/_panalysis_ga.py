#!/usr/bin/env/python

from __future__ import division

from googleapiclient.discovery import build
from oauth2client import client, file, tools

import pandas as pd
import numpy as np
import httplib2, os

from _query_parser import QueryParser

no_callback = client.OOB_CALLBACK_URN
default_scope = 'https://www.googleapis.com/auth/analytics.readonly'
default_token_file = os.path.join(os.path.dirname(__file__), 'analytics.dat')
default_secrets = os.path.join(os.path.dirname(__file__), 'client_secrets.json')

class OAuthDataReader(object):
    '''
    Abstract class for handling OAuth2 authentication using the Google
    oauth2client library
    '''
    def __init__(self, scope, token_file_name, redirect):
        '''
        Parameters
        ----------
        scope : str
            Designates the authentication scope
        token_file_name : str
            Location of cache for authenticated tokens
        redirect : str
            Redirect URL
        '''
        self._scope = scope
        self._redirect_url = redirect
        self._token_store = file.Storage(token_file_name)
        
        # NOTE:
        # This is a bit rough...
        self._flags = tools.argparser.parse_args(args=[])
        
    def _authenticate(self, secrets):
        '''
        Run the authentication process and return an authorized
        http object

        Parameters
        ----------
        secrets : str
            File name for client secrets

        Notes
        -----
        See google documention for format of secrets file
        '''
        flow = self._create_flow(secrets)
        
        credentials = self._token_store.get()
        
        if credentials is None or credentials.invalid:
            credentials = tools.run_flow(flow, self._token_store, self._flags)
            
        http = credentials.authorize(http=httplib2.Http())
        
        return http
    
    def _create_flow(self, secrets):
        '''
        Create an authentication flow based on the secrets file
        
        Parameters
        ----------
        secrets : str
            File name for client secrets
            
        Notes
        -----
        See google documentation for format of secrets file
        '''
        flow = client.flow_from_clientsecrets(secrets, scope=self._scope, \
                    message=tools.message_if_missing(secrets))
        
        return flow
    
    def _init_service(self, secrets):
        '''
        Build an authenticated google api request service using the given
        secrets file
        '''
        http = self._authenticate(secrets)
        
        return build('analytics', 'v3', http=http)
    
    def _reset_default_token_store(self):
        os.remove(default_token_file)
    
class GoogleAnalyticsQuery(OAuthDataReader):
    def __init__(self, scope=default_scope, token_file_name=default_token_file,
                 redirect=no_callback, secrets=default_secrets):
        '''
        Query the GA API with ease!  Simply obtain the 'client_secrets.json' file
        as usual and move it to the same directory as this file (default) or
        specify the file location when instantiating this class.
        
        If one does not exist, an 'analytics.dat' token file will also be
        created / read from the current working directory or whatever has 
        imported the class (default) or, one may specify the desired
        location when instantiating this class.  Note that this file requires
        write access, so you may need to either adjust the file permissions if
        using the default value.
        
        API queries must be provided as a dict. object, see the execute_query
        docstring for valid options.
        '''
        super(GoogleAnalyticsQuery, self).__init__(scope, token_file_name, redirect)
        self._service = self._init_service(secrets)
        
    def execute_query(self, as_dict=False, **query):
        '''
        Execute **query and translate it to a pandas.DataFrame object.
        
        Parameters:
        -----------
            as_dict : Boolean
                return the dict object provided by GA instead of the DataFrame
                object. Default = False
            query : dict.
                GA query, only with some added flexibility to be a bit sloppy. Adapted from
                https://developers.google.com/analytics/devguides/reporting/core/v3/reference
                The valid keys are:
                
            Key         Value   Reqd.   Summary
            --------------------------------------------------------------------------------
            ids         int     Y       The unique table ID of the form ga:XXXX or simply
                                        XXXX, where XXXX is the Analytics view (profile)
                                        ID for which the  query will retrieve the data.
            start_date  str     Y       Start date for fetching Analytics data. Requests can
                                        specify a start date formatted as YYYY-MM-DD, or as
                                        a relative date (e.g., today, yesterday, or NdaysAgo
                                        where N is a positive integer).
            end_date    str     Y       End date for fetching Analytics data. Request can 
                                        specify an end date formatted as YYYY-MM-DD, or as
                                        a relative date (e.g., today, yesterday, or NdaysAgo
                                        where N is a positive integer).
            metrics     list    Y       A list of comma-separated metrics, such as 
                                        'ga:sessions', 'ga:bounces', or simply 'sessions', etc.
            dimensions  list    N       A list of comma-separated dimensions for your
                                        Analytics data, such as 'ga:browser', 'ga:city',
                                        or simply 'browser', etc.
            sort        list    N       A list of comma-separated dimensions and metrics
                                        indicating the sorting order and sorting direction
                                        for the returned data.
            filters     list    N       Dimension or metric filters that restrict the data
                                        returned for your request. Multiple filters must
                                        be connected with 'and' or 'or' entries, with no
                                        default behaviour prescribed.
            segment     str     N       Segments the data returned for your request.
            samplingLevel str   N       The desired sampling level. Allowed Values:
                                        'DEFAULT' - Returns response with a sample size that
                                                    balances speed and accuracy.
                                        'FASTER' -  Returns a fast response with a smaller
                                                    sample size.
                                        'HIGHER_PRECISION' - Returns a more accurate response
                                                    using a large sample size, but this may 
                                                    result in the response being slower.
            start_index int     N       The first row of data to retrieve, starting at 1.
                                        Use this parameter as a pagination mechanism along
                                        with the max-results parameter.
            max_results int     N       The maximum number of rows to include in the response.
            output      str     N       The desired output type for the Analytics data returned
                                        in the response. Acceptable values are 'json' and 
                                        'dataTable'. Default is 'json'; if this option is
                                        used the 'as_dict' keyword argument is set
                                        to True and a dict object is returned.
            fields      list    N       Selector specifying a subset of fields to include in 
                                        the response.
                                        ***NOT CURRENTLY FORMAT-CHECKED***
            userIp      str     N       Specifies IP address of the end user for whom the API
                                        call is being made. Used to cap usage per IP.
                                        ***NOT CURRENTLY FORMAT-CHECKED***
            quotaUser   str     N       Alternative to userIp in cases when the user's IP
                                        address is unknown.
                                        ***NOT CURRENTLY FORMAT-CHECKED***
            access_token                DISABLED; behaviour is captured in class instantiation.
            callback                    DISABLED; behaviour is captured in class instantiation.
            prettyPrint                 DISABLED.
            key                         DISABLED.
                
        Returns:
        -----------
            reuslt : pd.DataFrame or dict
            query  : a copy of the (formatted) query actually used
            sampling : summay of sampling data if result contains sampled data
        '''
        sampling = None
        
        try:
            formatted_query = QueryParser().parse()
            
            try:
                if formatted_query['output']:
                    as_dict = True
                    
            except KeyError as e:
                pass
                
            ga_query = self._service.data().ga().get(**formatted_query)
            
        except TypeError as e:
            raise ValueError('Error making query: {0}'.format(e))
        
        res = ga_query.execute()
        
        if as_dict:
            return res, formatted_query, sampling
        
        else:
            # re-cast query result (dict) to a pd.DataFrame object
            cols = [col[u'name'][3:] for col in res[u'columnHeaders']]
            
            try:
                df = pd.DataFrame(res[u'rows'], columns=cols)
                
            except KeyError:
                df = pd.DataFrame(columns=cols)
                pass
            
            # TODO:
            # A tool to accurtely set the dtype for all columns of df would
            # be nice, but is probably far more effort than it's worth.
            # This will get the ball rolling, but the end user is likely
            # going to be stuck dealing with things on a per-case basis.
            def my_mapper(x):
                if x == u'INTEGER':
                    return int
                elif x == u'BOOLEAN':
                    return bool
                else:
                    return unicode
                
            for hdr in res[u'columnHeaders']:
                col = hdr[u'name'][3:]
                dtp = hdr[u'dataType']
                
                df[col] = df[col].apply(my_mapper(dtp))
                
            # Get the sampling data, if present
            if res['containsSampledData']:
                sampling = {'sampleSize'  : int(res['sampleSize']),
                            'sampleSpace' : int(res['sampleSpace'])}
                
            return df, formatted_query, sampling
        
        
class GoogleMCFQuery(OAuthDataReader):
    def __init__(self, scope=default_scope, token_file_name=default_token_file,
                 redirect=no_callback, secrets=default_secrets):
        '''
        Query the MCF API with ease!  Simply obtain the 'client_secrets.json' file
        as usual and move it to the same directory as this file (default) or
        specify the file location when instantiating this class.
        
        If one does not exist, an 'analytics.dat' token file will also be
        created / read from the current working directory or whatever has 
        imported the class (default) or, one may specify the desired
        location when instantiating this class.  Note that this file requires
        write access, so you may need to either adjust the file permissions if
        using the default value.
        
        API queries must be provided as a dict. object, see the execute_query
        docstring for valid options.
        '''
        super(GoogleMCFQuery, self).__init__(scope, token_file_name, redirect)
        self._service = self._init_service(secrets)
        
    def execute_query(self, as_dict=False, **query):
        '''
        Execute **query and translate it to a pandas.DataFrame object.
        
        Parameters:
        -----------
            as_dict : Boolean
                return the dict object provided by MCF instead of the DataFrame
                object. Default = False
            query : dict.
                MCF query, only with some added flexibility to be a bit sloppy. Adapted from
                https://developers.google.com/analytics/devguides/reporting/mcf/v3/reference
                The valid keys are:
                
            Key         Value   Reqd.   Summary
            --------------------------------------------------------------------------------
            ids         int     Y       The unique table ID of the form ga:XXXX or simply
                                        XXXX, where XXXX is the Analytics view (profile)
                                        ID for which the  query will retrieve the data.
            start_date  str     Y       Start date for fetching Analytics data. Requests can
                                        specify a start date formatted as YYYY-MM-DD, or as
                                        a relative date (e.g., today, yesterday, or NdaysAgo
                                        where N is a positive integer).
            end_date    str     Y       End date for fetching Analytics data. Request can 
                                        specify an end date formatted as YYYY-MM-DD, or as
                                        a relative date (e.g., today, yesterday, or NdaysAgo
                                        where N is a positive integer).
            metrics     list    Y       A list of comma-separated metrics, such as 
                                        'ga:sessions', 'ga:bounces', or simply 'sessions', etc.
            dimensions  list    N       A list of comma-separated dimensions for your
                                        Analytics data, such as 'ga:browser', 'ga:city',
                                        or simply 'browser', etc.
            sort        list    N       A list of comma-separated dimensions and metrics
                                        indicating the sorting order and sorting direction
                                        for the returned data.
            filters     list    N       Dimension or metric filters that restrict the data
                                        returned for your request. Multiple filters must
                                        be connected with 'and' or 'or' entries, with no
                                        default behaviour prescribed.
            samplingLevel str   N       The desired sampling level. Allowed Values:
                                        'DEFAULT' - Returns response with a sample size that
                                                    balances speed and accuracy.
                                        'FASTER' -  Returns a fast response with a smaller
                                                    sample size.
                                        'HIGHER_PRECISION' - Returns a more accurate response
                                                    using a large sample size, but this may 
                                                    result in the response being slower.
            start_index int     N       The first row of data to retrieve, starting at 1.
                                        Use this parameter as a pagination mechanism along
                                        with the max-results parameter.
            max_results int     N       The maximum number of rows to include in the response.
                
        Returns:
        -----------
            reuslt : pd.DataFrame or dict
            query  : a copy of the (formatted) query actually used
            sampling : summay of sampling data if result contains sampled data
        '''
        sampling = None
        
        try:
            formatted_query = QueryParser(prefix=u'mcf:').parse(**query)
            
            mcf_query = self._service.data().mcf().get(**formatted_query)
            
        except TypeError as e:
            raise ValueError('Error making query: {0}'.format(e))
        
        res = mcf_query.execute()
        
        if as_dict:
            return res, formatted_query, sampling
        
        else:
            # re-cast query result (dict) to a pd.DataFrame object
            rows = len(res['rows'])
            cols = [col['name'][4:] for col in res['columnHeaders']]
            
            try:
                df = pd.DataFrame(np.array(\
                        [i.values() for row in res['rows'] for i in row]).reshape(rows, len(cols)),
                            index=np.arange(rows), columns=cols)
                
            except KeyError:
                df = pd.DataFrame(columns=cols)
                pass
            
            # TODO:
            # A tool to accurtely set the dtype for all columns of df would
            # be nice, but is probably far more effort than it's worth.
            # This will get the ball rolling, but the end user is likely
            # going to be stuck dealing with things on a per-case basis.
            def my_mapper(x):
                if x == u'INTEGER':
                    return int
                elif x == u'CURRENCY':
                    return float
                elif x == u'BOOLEAN':
                    return bool
                else:
                    return unicode
                
            for hdr in res[u'columnHeaders']:
                col = hdr[u'name'][4:]
                dtp = hdr[u'dataType']
                
                df[col] = df[col].apply(my_mapper(dtp))
                
            # Get the sampling data, if present
            if res['containsSampledData']:
                sampling = {'sampleSize'  : int(res['sampleSize']),
                            'sampleSpace' : int(res['sampleSpace'])}
                
            return df, formatted_query, sampling