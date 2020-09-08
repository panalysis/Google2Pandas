import pandas as pd
import numpy as np

import httplib2
import os

from googleapiclient.discovery import build
from oauth2client import client, file, tools
from oauth2client.service_account import ServiceAccountCredentials
from sys import stdout


from ._query_parser import QueryParser

no_callback = client.OOB_CALLBACK_URN
default_scope = 'https://www.googleapis.com/auth/analytics.readonly'
default_discovery = 'https://analyticsreporting.googleapis.com/$discovery/rest'
default_token_file = os.path.join(os.path.dirname(__file__), 'analytics.dat')
default_secrets_v3 = os.path.join(os.path.dirname(__file__), 'client_secrets_v3.json')
default_secrets_v4 = os.path.join(os.path.dirname(__file__), 'client_secrets_v4.json')


class OAuthDataReaderV4:
    '''
    Abstract class for handling OAuth2 authentication using the Google
    oauth2client library and the V4 Analytics API
    '''
    def __init__(self, scope, discovery_uri):
        '''
        Parameters:
        -----------
            secrets : string
                Path to client_secrets.json file. p12 formatted keys not
                supported at this point.
            scope : list or string
                Designates the authentication scope(s).
            discovery_uri : tuple or string
                Designates discovery uri(s)
        '''
        self._scope = scope
        self._discovery = discovery_uri
        self._api = 'v4'

    def _init_service(self, secrets):
        creds = ServiceAccountCredentials\
            .from_json_keyfile_name(secrets,
                scopes=self._scope
            )

        http = creds.authorize(httplib2.Http())

        # silence log warnigns as suggested by
        # https://github.com/googleapis/google-api-python-client/issues/299
        return build('analytics', self._api,
            http=http,
            discoveryServiceUrl=self._discovery,
            cache_discovery=False
        )

class OAuthDataReader:
    '''
    Abstract class for handling OAuth2 authentication using the Google
    oauth2client library
    '''
    def __init__(self, scope, token_file_name, redirect):
        '''
        Parameters:
        -----------
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
        self._api = 'v3'

        # NOTE:
        # This is a bit rough...
        self._flags_ = tools.argparser.parse_args(args=[])

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
            credentials = tools.run_flow(flow, self._token_store, self._flags_)

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
        flow = client.flow_from_clientsecrets(secrets,
                                              scope=self._scope,
                                              message=tools.message_if_missing(secrets))

        return flow

    def _init_service(self, secrets):
        '''
        Build an authenticated google api request service using the given
        secrets file
        '''
        http = self._authenticate(secrets)

        return build('analytics', self._api, http=http)

    def _reset_default_token_store(self):
        os.remove(default_token_file)


class GoogleAnalyticsQuery(OAuthDataReader):
    def __init__(self,
                 scope=default_scope,
                 token_file_name=default_token_file,
                 redirect=no_callback,
                 secrets=default_secrets_v3):
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
        super(GoogleAnalyticsQuery, self).__init__(scope,
                                                   token_file_name,
                                                   redirect)

        self._service = self._init_service(secrets)

    def execute_query(self, as_dict=False, all_results=False, **query):
        '''
        Execute **query and translate it to a pandas.DataFrame object.

        Parameters:
        -----------
            as_dict : Boolean
                Return the dict object provided by GA instead of the DataFrame
                object. Default = False
            all_results : Boolean
                Obtain the full query results availble from GA (up to sampling limit).
                This can be VERY time / bandwidth intensive! Default = False
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
            result : pd.DataFrame or dict
            metadata : summary data supplied with query result
        '''
        try:
            formatted_query = QueryParser().parse(**query)

            try:
                if formatted_query['output']:
                    as_dict = True

            except KeyError as e:
                pass

            ga_query = self._service.data().ga().get(**formatted_query)

        except TypeError as e:
            raise ValueError(f'Error making query: {e}')

        res = ga_query.execute()

        # Fix the 'query' field to be useful to us
        for key in list(res['query'].keys()):
            res['query'][key.replace('-', '_')] = res['query'].pop(key)

        if as_dict:
            return res

        else:
            # re-cast query result (dict) to a pd.DataFrame object
            cols = [col['name'][3:] for col in res['columnHeaders']]

            try:
                df = pd.DataFrame(res['rows'], columns=cols)

                # Some kludge to optionally get the the complete query result
                # up to the sampling limit
                if all_results:
                    print('Obtianing full data set (up to sampling limit).')
                    print('This can take a VERY long time!')

                    more = True
                    temp_qry = formatted_query.copy()

                    while more:
                        try:
                            temp_qry['start_index'] = \
                                res['nextLink'].split('start-index=')[1].split('&')[0]

                            # Monitor progress
                            curr = int(temp_qry['start_index'])
                            block = int(res['itemsPerPage'])
                            total = res['totalResults']

                            stdout.write('\rGetting rows {0} - {1} of {2}'.\
                                format(curr, curr + block - 1, total))
                            stdout.flush()

                            temp_res = self._service.data().ga().get(**temp_qry).execute()
                            temp_df =  pd.DataFrame(temp_res['rows'], columns=cols)

                            df = pd.concat((df, temp_df), ignore_index=True)

                            res['nextLink'] = temp_res['nextLink']

                        except KeyError:
                            more = False

            except KeyError:
                df = pd.DataFrame(columns=cols)
                pass

            # TODO:
            # A tool to accurtely set the dtype for all columns of df would
            # be nice, but is probably far more effort than it's worth.
            # This will get the ball rolling, but the end user is likely
            # going to be stuck dealing with things on a per-case basis.
            # We should be able to leverage the resp2frame code below to 
            # improve the the handling of conversion here.
            def my_mapper(x):
                if x == 'INTEGER':
                    return int
                elif x == 'BOOLEAN':
                    return bool
                else:
                    # this should work with both 2.7 and 3.4
                    if isinstance(x, str):
                        return str

                    else:
                        return str

            for hdr in res['columnHeaders']:
                col = hdr['name'][3:]
                dtp = hdr['dataType']

                df[col] = df[col].apply(my_mapper(dtp))

            # Return the summary info as well
            try:
                res.pop('rows')

            except KeyError:
                pass

            res.pop('columnHeaders')

            return df, res

class GoogleAnalyticsQueryV4(OAuthDataReaderV4):
    def __init__(self,
                 scope=default_scope,
                 discovery=default_discovery,
                 secrets=default_secrets_v4):
        '''
        Query the GA API with ease!  Simply obtain the 'client_secrets.json' file
        as usual and move it to the same directory as this file (default) or
        specify the file location when instantiating this class.

        *** Different for API V4 ***
        The authentication process is different for the V4 API. In terms of
        this class, the primary difference is that the 'analytics.dat' file is
        no longer required, but adding the service account email to the actual
        service account is. The email address will be of the form

            xxx@report-automation-1316.iam.gserviceaccount.com

        and is in the JSON key provided. Refer to

            https://developers.google.com/analytics/devguides/reporting/core/v4/quickstart/service-py

        for additional details.

        TODO:
        At the very least, the 'fields' parameter should be included here:

            https://developers.google.com/analytics/devguides/reporting/core/v4/parameters
        '''
        super(GoogleAnalyticsQueryV4, self).__init__(scope, discovery)
        self._service = self._init_service(secrets)

    def execute_query(self, query, as_dict=False, all_results=True):
        '''
        Execute **query and translate it to a pandas.DataFrame object.

        Parameters:
        -----------
            query: dict
                Refer to:

                    https://developers.google.com/analytics/devguides/reporting/core/v4/rest/v4/reports

                for guidance. Automatic parsing has been deprecated in V4.
            as_dict : Boolean
                Return the dict object provided by GA instead of the DataFrame
                object. Default = False
            all_results : Boolean
                Get all the data for the query instead of the 1000-row limit.
                Defualt = True

        Returns:
        -----------
            df : pandas.DataFrame
                Reformatted response to **query.
        '''
        if all_results:
            out = {'reports' : []}

            while True:
                response = self._service.reports().batchGet(body=query).execute()
                out['reports'] += response['reports']

                tkn = response.get('reports', [])[0].get('nextPageToken', '')
                if tkn:
                    query['reportRequests'][0].update({'pageToken' : tkn})

                else:
                    _ = query.pop('reportRequests')[0].get('pageToken')
                    break

        else:
            out = self._service.reports().batchGet(body=query).execute()

        if as_dict:
            return out

        else:
            return self.resp2frame(out)

    @staticmethod
    def resp2frame(resp):
        # return object
        out = pd.DataFrame()
        # GA data type to data frame conversion
        lookup = {
          'INTEGER'     : 'int32',
          'FLOAT'       : 'float32',
          'CURRENCY'    : 'float32',
          'PERCENT'     : 'float32',
          'TIME'        : 'object',
          'STRING'      : 'object'
        }

        # Loop through reports and get metrics and dimensions
        for report in resp.get('reports', []):
            col_hdrs = report.get('columnHeader', {})
            # Get the initial dimensions
            cols = col_hdrs.get('dimensions', [])
            metric_cols = []

            if 'metricHeader' in col_hdrs:
                metrics = col_hdrs.get('metricHeader', {}).get('metricHeaderEntries', [])
                cols_data_type = {}

                for m in metrics:
                    # Get each metric and the data type
                    cols += [m.get('name')]
                    cols_data_type[m.get('name')] = lookup[m.get('type')]

            # Take out any "ga:" prefixes
            cols = list(map(lambda x: x.replace('ga:', ''), cols))

            # Set the dataframe with the column names
            df = pd.DataFrame(columns=cols)

            # Get the rows from the GA report
            rows = report.get('data', {}).get('rows')

            # Let's loop through the rows to get the dimensions and metrics to row list
            for row in rows:
                row_list = row.get('dimensions', [])

                if 'metrics' in list(row.keys()):
                    metrics = row.get('metrics', [])
                    for m in metrics:
                        row_list = row_list + m.get('values')

                # Make each row an enumerated dictionary with index value starting
                # at 0
                drow = {}
                for i, c in enumerate(cols):
                    drow.update({c : row_list[i]})

                # Concatanate the row to the overall list
                df = pd.concat((df, pd.DataFrame(drow, index=[0])),
                               ignore_index=True)

            # Copy the dataframe to the returning object
            out = pd.concat((out, df), ignore_index=True)

            # Convert the object types to the inferred ones
            out = out.apply(pd.to_numeric, errors='ignore', axis=1)

            # Explicitly convert date back to a date object
            if 'date' in out.columns:
                out['date'] = pd.to_datetime(out['date'], format='%Y%m%d')

        return out




# UPDATE 2020-09-08:
#   I don't think this class should actully be here; if this even works anymore,
#   the more usefult thing to do is something like:
#
#   class MyExtendedClass(OAuthDataReader):
#       def __init__(self, *args, **kwargs):
#           super().__init__(*args)
#
#   and define your methods as required.
#
#
#
# class GoogleMCFQuery(OAuthDataReader):
#     def __init__(self,
#                  scope=default_scope,
#                  token_file_name=default_token_file,
#                  redirect=no_callback,
#                  secrets=default_secrets_v3):
#         '''
#         Query the MCF API with ease!  Simply obtain the 'client_secrets.json' file
#         as usual and move it to the same directory as this file (default) or
#         specify the file location when instantiating this class.

#         If one does not exist, an 'analytics.dat' token file will also be
#         created / read from the current working directory or whatever has
#         imported the class (default) or, one may specify the desired
#         location when instantiating this class.  Note that this file requires
#         write access, so you may need to either adjust the file permissions if
#         using the default value.

#         API queries must be provided as a dict. object, see the execute_query
#         docstring for valid options.
#         '''
#         super(GoogleMCFQuery, self).__init__(scope, token_file_name, redirect)
#         self._service = self._init_service(secrets)

#     def execute_query(self, as_dict=False, **query):
#         '''
#         Execute **query and translate it to a pandas.DataFrame object.

#         Parameters:
#         -----------
#             as_dict : Boolean
#                 return the dict object provided by MCF instead of the DataFrame
#                 object. Default = False
#             query : dict.
#                 MCF query, only with some added flexibility to be a bit sloppy. Adapted from
#                 https://developers.google.com/analytics/devguides/reporting/mcf/v3/reference
#                 The valid keys are:

#             Key         Value   Reqd.   Summary
#             --------------------------------------------------------------------------------
#             ids         int     Y       The unique table ID of the form ga:XXXX or simply
#                                         XXXX, where XXXX is the Analytics view (profile)
#                                         ID for which the  query will retrieve the data.
#             start_date  str     Y       Start date for fetching Analytics data. Requests can
#                                         specify a start date formatted as YYYY-MM-DD, or as
#                                         a relative date (e.g., today, yesterday, or NdaysAgo
#                                         where N is a positive integer).
#             end_date    str     Y       End date for fetching Analytics data. Request can
#                                         specify an end date formatted as YYYY-MM-DD, or as
#                                         a relative date (e.g., today, yesterday, or NdaysAgo
#                                         where N is a positive integer).
#             metrics     list    Y       A list of comma-separated metrics, such as
#                                         'ga:sessions', 'ga:bounces', or simply 'sessions', etc.
#             dimensions  list    N       A list of comma-separated dimensions for your
#                                         Analytics data, such as 'ga:browser', 'ga:city',
#                                         or simply 'browser', etc.
#             sort        list    N       A list of comma-separated dimensions and metrics
#                                         indicating the sorting order and sorting direction
#                                         for the returned data.
#             filters     list    N       Dimension or metric filters that restrict the data
#                                         returned for your request. Multiple filters must
#                                         be connected with 'and' or 'or' entries, with no
#                                         default behaviour prescribed.
#             samplingLevel str   N       The desired sampling level. Allowed Values:
#                                         'DEFAULT' - Returns response with a sample size that
#                                                     balances speed and accuracy.
#                                         'FASTER' -  Returns a fast response with a smaller
#                                                     sample size.
#                                         'HIGHER_PRECISION' - Returns a more accurate response
#                                                     using a large sample size, but this may
#                                                     result in the response being slower.
#             start_index int     N       The first row of data to retrieve, starting at 1.
#                                         Use this parameter as a pagination mechanism along
#                                         with the max-results parameter.
#             max_results int     N       The maximum number of rows to include in the response.

#         Returns:
#         -----------
#             result : pd.DataFrame or dict
#             metadata : summary data supplied with query result
#         '''
#         try:
#             formatted_query = QueryParser(prefix='mcf:').parse(**query)

#             mcf_query = self._service.data().mcf().get(**formatted_query)

#         except TypeError as e:
#             raise ValueError(f'Error making query: {e}')

#         res = mcf_query.execute()

#         # Fix the 'query' field to be useful to us
#         for key in list(res['query'].keys()):
#             res['query'][key.replace('-', '_')] = res['query'].pop(key)

#         if as_dict:
#             return res

#         else:
#             # re-cast query result (dict) to a pd.DataFrame object
#             rows = len(res['rows'])
#             cols = [col['name'].replace('mcf:','') for col in res['columnHeaders']]

#             try:
#                 df = pd.DataFrame(np.array(\
#                         [list(i.values()) for row in res['rows'] for i in row]).\
#                             reshape(rows, len(cols)), columns=cols)

#             except KeyError:
#                 df = pd.DataFrame(columns=cols)
#                 pass

#             # TODO:
#             # A tool to accurtely set the dtype for all columns of df would
#             # be nice, but is probably far more effort than it's worth.
#             # This will get the ball rolling, but the end user is likely
#             # going to be stuck dealing with things on a per-case basis.
#             # We should be able to leverage the resp2frame code above to 
#             # improve the the handling of conversion here.
#             def my_mapper(x):
#                 if x == 'INTEGER':
#                     return int
#                 elif x == 'CURRENCY':
#                     return float
#                 elif x == 'BOOLEAN':
#                     return bool
#                 else:
#                     # this should work with both 2.7 and 3.4
#                     if isinstance(x, str):
#                         return str

#                     else:
#                         return str

#             for hdr in res['columnHeaders']:
#                 col = hdr['name'].replace('mcf:','')
#                 dtp = hdr['dataType']

#                 df[col] = df[col].apply(my_mapper(dtp))

#             # Return the summary info as well
#             try:
#                 res.pop('rows')

#             except KeyError:
#                 pass

#             res.pop('columnHeaders')

#             return df, res
