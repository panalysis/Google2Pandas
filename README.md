# README

Google2Pandas ~~will~~ may eventually be a set of tools that will allow for easy querying
of various google database products (Analytics, etc.) with the results returned as 
pandas.DataFrame objects (http://pandas.pydata.org/).

At this point, only queries to Google Analytics and the Multi-Channel Funnels Reporting via
the core reporting API are supported.

## Nomenclature
Suggested usage: 

```
from google2pandas import *
```

## Quick Setup
Install the latest version via pip:

```
sudo pip install Google2Pandas
```

or install the latest development version via:

```
sudo pip install git+https://github.com/panalysis/Google2Pandas
```

You will first need to enable the Analytics API, in particular you will
need to follow [Step 1](https://developers.google.com/analytics/devguides/reporting/core/v3/quickstart/installed-py) here.

Place the `client_secrets_v3.json` file in your `dist-packages/google2pandas/` directory,
and you're ready to go!  Note that if this package has been installed system-wide
(default), you will likely need to adjust the permissions/ownership of 
`client_secrets_v3.json` as well as the created `analytics.dat` token file. In 
particular, if you wish to create a system-wide token file (by default the class
looks in `/path/to/your/dist-packages/google2pandas/analytics.dat`) you will likely
need to instantiate the `GoogleAnalyticsQuery` class specifying a local location
for the token file, and manually relocate it later.

Alternatively, store your credentials anywhere you like and simply pass a pointer
to `client_secrets_v3.json` and `analytics.dat` when instantiating the class.

### Quick Demo
```
from google2pandas import *

query = {\
    'ids'           : <valid_ids>,
    'metrics'       : 'pageviews',
    'dimensions'    : ['date', 'pagePath', 'browser'],
    'filters'       : ['pagePath=~iPhone', 'and', 'browser=~Firefox'],
    'start_date'    : '8daysAgo',
    'max_results'   : 10}
    
conn = GoogleAnalyticsQuery(
        token_file_name='my_analytics.dat',
	secrets='my_client_secrets_v3.json')
df, metadata = conn.execute_query(**query)
```

## New and Improved (more of a work in progess really)
Support has now been added for the GA Reporting API V4 as suggested in [issue #21](https://github.com/panalysis/Google2Pandas/issues/21) via the `GoogleAnalyticsQueryV4`
class. The support is rather rough for now, the primary reason being that since I'm
not working with GA much at all these days I do not have the time to fully learn the
features present in the new API.

For now, what this means is that there is zero parsing of the queries provided,
it's down to the user to structure them correctly. As well, no guarantees are
provided as to the ability to of the `resp2frame` method to convert the JSON object
from GA to a `pandas.DataFrame` object in a manner that is generically robust. The
`as_dict` keyword argument causes the restructuring step to be skipped; if you find
room for improvements please do not hesitate to make a PR with your
suggestions!

To use this module, one needs to follow the [new setup process](https://developers.google.com/analytics/devguides/reporting/core/v4/quickstart/service-py)
to enable acces. No more `analytics.dat` file, instead one needs to simply add the
generated email address to the GA view you wish to access.

I also suggest naming the `client_secrets` file to something that indicates it
is for the V4 API, as it is quite a different thing than the V3 version (default
behaviour is to look for `client_secrets_v4.json` in `dist-packages/google2pandas/`).

### Quick Demo
```
from google2pandas import *

query = {
    'reportRequests': [{
        'viewId' : <valid_ids>,
        
        'dateRanges': [{
            'startDate' : '8daysAgo',
            'endDate'   : 'today'}],
            
        'dimensions' : [
            {'name' : 'ga:date'}, 
            {'name' : 'ga:pagePath'},
            {'name' : 'ga:browser'}],
            
        'metrics'   : [
            {'expression' : 'ga:pageviews'}],
            
        'dimensionFilterClauses' : [{
            'operator' : 'AND',
            'filters'  : [
                {'dimensionName' : 'ga:browser',
                 'operator' : 'REGEXP',
                 'expressions' : ['Firefox']},
                 
                {'dimensionName' : 'ga:pagePath',
                 'operator' : 'REGEXP',
                 'expressions' : ['iPhone']}]
        }]
    }]
}
    
# Assume we have placed our client_secrets_v4.json file in the current
# working directory.

conn = GoogleAnalyticsQueryV4(secrets='my_client_secrets_v4.json')
df = conn.execute_query(query)
```
