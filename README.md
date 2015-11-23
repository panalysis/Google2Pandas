#README#

Google2Pandas will eventually be a set of tools that will allow for easy querying
of various google database products (Analytics, BigQuery, etc.) with the results
returned as pandas.DataFrame objects (http://pandas.pydata.org/).

At this point, only queries to Google Analytics via the core reporting API are
supported.

##Nomenclature##
Suggested usage: 

```
from google2pandas import *
```

##Quick Setup##
Install the latest version via pip:

```
sudo pip install git+https://github.com/DeliciousHair/Google2Pandas
```

You will first need to enable the Analytics API, in particular you will
need to follow [Step 1](https://developers.google.com/analytics/devguides/reporting/core/v3/quickstart/installed-py) here.

Place the `client_secrets.json` in your `dist-packages/google2pandas/` directory,
and you're ready to go!  Note that if this package has been installed system-wide
(default), you will likely need to adjust the permissions/ownership of 
`client_secrets.json` as well as the created `analytics.dat` token file. In 
particular, if you wish to create a system-wide token file (by default the class
looks in `/path/to/your/dist-packages/google2pandas/analytics.dat`) you will likely
need to instantiate the `GoogleAnalyticsQuery` class specifying a local location
for the token file, and manually relocate it later.

###Quick Demo###
```
from google2pandas import *

query = {\
    'ids'           : <valid_ids>,
    'metrics'       : 'pageviews',
    'dimensions'    : ['date', 'pagePath', 'browser'],
    'filters'       : ['pagePath=~iPhone', 'and', 'browser=~Firefox'],
    'start_date'    : '8daysAgo',
    'max_results'   : 10}
    
ga = GoogleAnalyticsQuery(token_file_name='analytics.dat')
df, formatted_qry = ga.execute_query(**query)
```
