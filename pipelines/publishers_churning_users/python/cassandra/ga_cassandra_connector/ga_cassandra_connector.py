"""Google Analytics Reporting API V4 Connector for the MorphL project"""

# This connector is intended to run inside of a Docker container.
# Install Python dependencies:
# pip install oauth2client google-api-python-client cassandra-driver
# In the same directory you will find in a .cql file all the prerequisite Cassandra table definitions.
# The following environment variables need to be set before executing this connector:
# DAY_OF_DATA_CAPTURE=2018-07-27
# MORPHL_SERVER_IP_ADDRESS
# MORPHL_CASSANDRA_PASSWORD
# KEY_FILE_LOCATION
# VIEW_ID

from time import sleep
from json import dumps
from os import getenv
from sys import exc_info

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

class CassandraPersistence:
    def __init__(self):
        self.DAY_OF_DATA_CAPTURE = getenv('DAY_OF_DATA_CAPTURE')
        self.MORPHL_SERVER_IP_ADDRESS = getenv('MORPHL_SERVER_IP_ADDRESS')
        self.MORPHL_CASSANDRA_USERNAME = 'morphl'
        self.MORPHL_CASSANDRA_PASSWORD = getenv('MORPHL_CASSANDRA_PASSWORD')
        self.MORPHL_CASSANDRA_KEYSPACE = 'morphl'
        self.CASS_REQ_TIMEOUT = 3600.0
        self.auth_provider = PlainTextAuthProvider(
            username=self.MORPHL_CASSANDRA_USERNAME, password=self.MORPHL_CASSANDRA_PASSWORD)
        self.cluster = Cluster(
            contact_points=[self.MORPHL_SERVER_IP_ADDRESS], auth_provider=self.auth_provider)
        self.session = self.cluster.connect(self.MORPHL_CASSANDRA_KEYSPACE)
        self.prep_stmts = {}
        partition_key_of_1_list = ['client_ids', 'desktop_device', 'mobile_device', 'system_data']
        partition_key_of_2_list = ['geo_content', 'goal_1_thru_10_completions', 'goal_11_thru_20_completions', 'session_ids', 'traffic_sources']
        partition_key_of_3_list = ['events', 'page_tracking', 'site_speed']
        template_key_of_1 = 'INSERT INTO ga_{} (client_id,day_of_data_capture,json_data) VALUES (?,?,?)'
        template_key_of_2 = 'INSERT INTO ga_{} (client_id,day_of_data_capture,session_id,json_data) VALUES (?,?,?,?)'
        template_key_of_3 = 'INSERT INTO ga_{} (client_id,day_of_data_capture,tz_time,session_id,json_data) VALUES (?,?,?,?,?)'
        for report_type in partition_key_of_1_list:
            self.prep_stmts[report_type] = self.session.prepare(template_key_of_1.format(report_type))
        for report_type in partition_key_of_2_list:
            self.prep_stmts[report_type] = self.session.prepare(template_key_of_2.format(report_type))
        for report_type in partition_key_of_3_list:
            self.prep_stmts[report_type] = self.session.prepare(template_key_of_3.format(report_type))
        self.partition_key_of_1_set = set(partition_key_of_1_list)
        self.partition_key_of_2_set = set(partition_key_of_2_list)
        self.partition_key_of_3_set = set(partition_key_of_3_list)

    def persist_dict_record(self, report_type, dict_record):
        client_id = dict_record['dimensions'][0] if dict_record['dimensions'][0].startswith('GA') else 'UNKNOWN'
        json_data = dumps(dict_record)
        if report_type in self.partition_key_of_1_set:
            bind_list = [client_id, self.DAY_OF_DATA_CAPTURE, json_data]
            return {'cassandra_future': self.session.execute_async(self.prep_stmts[report_type], bind_list, timeout=self.CASS_REQ_TIMEOUT),
                    'client_id': client_id}
        if report_type in self.partition_key_of_2_set:
            session_id = dict_record['dimensions'][1]
            bind_list = [client_id, self.DAY_OF_DATA_CAPTURE, session_id, json_data]
            return {'cassandra_future': self.session.execute_async(self.prep_stmts[report_type], bind_list, timeout=self.CASS_REQ_TIMEOUT),
                    'client_id': client_id,
                    'session_id': session_id}
        if report_type in self.partition_key_of_3_set:
            session_id = dict_record['dimensions'][1]
            tz_time = dict_record['dimensions'][2]
            bind_list = [client_id, self.DAY_OF_DATA_CAPTURE, tz_time, session_id, json_data]
            return {'cassandra_future': self.session.execute_async(self.prep_stmts[report_type], bind_list, timeout=self.CASS_REQ_TIMEOUT),
                    'client_id': client_id,
                    'session_id': session_id}

class GoogleAnalytics:
    def __init__(self):
        self.SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
        self.KEY_FILE_LOCATION = getenv('KEY_FILE_LOCATION')
        self.VIEW_ID = getenv('VIEW_ID')
        self.API_PAGE_SIZE = 10000
        self.DAY_OF_DATA_CAPTURE = getenv('DAY_OF_DATA_CAPTURE')
        self.start_date = self.DAY_OF_DATA_CAPTURE
        self.end_date = self.DAY_OF_DATA_CAPTURE
        self.analytics = None
        self.store = CassandraPersistence()

    # Initializes an Analytics Reporting API V4 service object.
    def authenticate(self):
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            self.KEY_FILE_LOCATION, self.SCOPES)
        # Build the service object.
        self.analytics = build('analyticsreporting',
                               'v4', credentials=credentials)

    # Transform list of dimensions names into objects with a 'name' property.
    def format_dimensions(self, dims):
        return [ {'name': 'ga:' + dim} for dim in dims ]

    # Transform list of metrics names into objects with an 'expression' property.
    def format_metrics(self, metrics):
        return [ {'expression': 'ga:' + metric} for metric in metrics ]

    # Make request to the GA reporting API and return paginated results.
    def run_report_and_store(self, report_type, dimensions, metrics, dimensions_filters=None, metrics_filters=None):
        """Queries the Analytics Reporting API V4 and stores the results in a datastore.

        Args:
          analytics: An authorized Analytics Reporting API V4 service object
          report_type: The type of data being requested
          dimensions: A list with the GA dimensions
          metrics: A list with the metrics
          dimensions_filters: A list with the GA dimensions filters
          metrics_filters: A list with the GA metrics filters
        """
        query_params = {
            'viewId': self.VIEW_ID,
            'dateRanges': [{'startDate': self.start_date, 'endDate': self.end_date}],
            'dimensions': self.format_dimensions(dimensions),
            'metrics': self.format_metrics(metrics),
            'pageSize': self.API_PAGE_SIZE,
        }

        if dimensions_filters is not None:
            query_params['dimensionFilterClauses'] = dimensions_filters

        if metrics_filters is not None:
            query_params['metricFilterClauses'] = metrics_filters

        complete_responses_list = []
        reports_object = self.analytics.reports()
        page_token = None
        while True:
            sleep(0.1)
            if page_token:
                query_params['pageToken'] = page_token
            data_chunk = reports_object.batchGet(body={'reportRequests': [query_params]}).execute()
            partial_rl = []
            try:
                partial_rl = [ self.store.persist_dict_record(report_type, dict_record) for dict_record in data_chunk['reports'][0]['data']['rows'] ]
            except Exception as ex:
                print('BEGIN EXCEPTION')
                print(report_type)
                print(exc_info()[0])
                print(str(ex))
                print(dumps(data_chunk['reports'][0]))
                print('END EXCEPTION')
            complete_responses_list.extend(partial_rl)
            page_token = data_chunk['reports'][0].get('nextPageToken')
            if not page_token:
                break

        # Wait for acks from Cassandra
        [ cr['cassandra_future'].result() for cr in complete_responses_list ]

        return complete_responses_list

    # Get users / unique client ids
    def store_client_ids(self):
        return self.run_report_and_store('client_ids', ['dimension1'], ['users'])

    # Get sessions / unique session ids
    def store_session_ids(self, dimensions_filters):
        dimensions = ['dimension1', 'dimension2', 'sessionDurationBucket']
        metrics = ['pageViews']

        return self.run_report_and_store('session_ids', dimensions, metrics, dimensions_filters)

    # Get platform device
    def store_platform_devices(self, dimensions_filters):
        # Get data for desktop devices
        dimensions = [
            'dimension1',
            'browser',
            'browserVersion',
            'operatingSystem',
            'operatingSystemVersion'
        ]
        desktop_devices = self.run_report_and_store('desktop_device', dimensions, ['users'], dimensions_filters)
        # Get data for mobile devices
        dimensions = [
            'dimension1',
            'mobileDeviceBranding',
            'mobileDeviceModel',
            'mobileInputSelector',
            'mobileDeviceInfo'
        ]
        mobile_devices = self.run_report_and_store('mobile_device', dimensions, ['users'], dimensions_filters)

        return {'desktop_devices': desktop_devices, 'mobile_devices': mobile_devices}

    # Get system data
    def store_system_data(self, dimensions_filters):
        dimensions = ['dimension1', 'language', 'screenResolution']
        metrics = ['sessions']

        return self.run_report_and_store('system_data', dimensions, metrics, dimensions_filters)

    # Get goals conversions
    def store_goals(self, dimensions_filters):
        goals_metrics_filter = [
            {
                'filters': {
                    'metricName': 'ga:goalCompletionsAll',
                    'operator': 'GREATER_THAN',
                    'comparisonValue': "0",
                },
            },
        ]
        dimensions = ['dimension1', 'dimension2']
        # Get goals from 1 to 20 in two requests
        response_dict = {}
        for i in [1, 11]:
            metrics = [ f'goal{str(i)}Completions' for i in range(i, i + 10) ]
            report_type = f'goal_{str(i)}_thru_{str(i+9)}_completions'
            response_dict[report_type] = self.run_report_and_store(report_type, dimensions, metrics, dimensions_filters, goals_metrics_filter)

        return response_dict

    # Get events
    def store_events(self, dimensions_filters):
        dimensions = ['dimension1', 'dimension2', 'dimension3', 'eventCategory',
                      'eventAction', 'eventLabel', 'pagePath']
        metrics = ['sessions', 'totalEvents',
                   'eventValue', 'sessionsWithEvent']

        return self.run_report_and_store('events', dimensions, metrics, dimensions_filters)

    # Get page tracking
    def store_page_tracking(self, dimensions_filters):
        dimensions = ['dimension1', 'dimension2', 'dimension3',
                      'hostname', 'pagePath', 'landingPagePath', 'secondPagePath', 'exitPagePath', 'previousPagePath']
        metrics = ['sessions', 'pageValue',
                   'entrances', 'pageviews', 'uniquePageviews', 'timeOnPage', 'exits']

        return self.run_report_and_store('page_tracking', dimensions, metrics, dimensions_filters)

    # Get site speed
    def store_site_speed(self, dimensions_filters):
        dimensions = ['dimension1', 'dimension2', 'dimension3',
                      'hostname', 'pagePath', 'landingPagePath', 'secondPagePath', 'exitPagePath', 'previousPagePath']
        metrics = ['sessions', 'pageLoadTime',
                   'pageLoadSample', 'domainLookupTime', 'pageDownloadTime', 'redirectionTime',
                   'serverConnectionTime', 'serverResponseTime', 'domInteractiveTime', 'domContentLoadedTime']

        return self.run_report_and_store('site_speed', dimensions, metrics, dimensions_filters)

    # Get geo content
    def store_geo_content(self, dimensions_filters):
        dimensions = ['dimension1', 'dimension2',
                      'continent', 'country', 'region', 'city', 'latitude', 'longitude', 'regionIsoCode']
        metrics = ['sessions']

        return self.run_report_and_store('geo_content', dimensions, metrics, dimensions_filters)

    # Get traffic sources
    def store_traffic_sources(self, dimensions_filters):
        dimensions = ['dimension1', 'dimension2',
                      'referralPath', 'fullReferrer', 'source', 'medium', 'keyword', 'socialNetwork', 'hasSocialSourceReferral']
        metrics = ['sessions']

        return self.run_report_and_store('traffic_sources', dimensions, metrics, dimensions_filters)

    def run(self):
        self.authenticate()
        client_ids = [ cr['client_id'] for cr in self.store_client_ids() ]
        # Create filter by client ids
        dim1_filter = [
            {
                'filters': {
                    'dimensionName': 'ga:dimension1',
                    'operator': 'IN_LIST',
                    'expressions': client_ids,
                },
            },
        ]
        session_ids = [ cr['session_id'] for cr in self.store_session_ids(dim1_filter) ]
        # Create filter by session ids
        dim2_filter = [
            {
                'filters': {
                    'dimensionName': 'ga:dimension2',
                    'operator': 'IN_LIST',
                    'expressions': session_ids,
                },
            },
        ]
        # Get other metrics / dimensions
        platform_devices = self.store_platform_devices(dim1_filter)
        system_data = self.store_system_data(dim1_filter)
        goals = self.store_goals(dim1_filter)
        events = self.store_events(dim2_filter)
        page_tracking = self.store_page_tracking(dim2_filter)
        site_speed = self.store_site_speed(dim2_filter)
        geo_content = self.store_geo_content(dim2_filter)
        traffic_sources = self.store_traffic_sources(dim2_filter)

def main():
    google_analytics = GoogleAnalytics()
    google_analytics.run()

if __name__ == '__main__':
    main()
