"""Google Analytics Reporting API V4 Connector for the MorphL project"""

from time import sleep
from json import dumps
from os import getenv
from sys import exc_info

from apiclient.discovery import build
from google.oauth2 import service_account

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider


class CassandraPersistence:
    def __init__(self):
        self.DAY_OF_DATA_CAPTURE = getenv('DAY_OF_DATA_CAPTURE')
        self.MORPHL_SERVER_IP_ADDRESS = getenv('MORPHL_SERVER_IP_ADDRESS')
        self.MORPHL_CASSANDRA_USERNAME = getenv('MORPHL_CASSANDRA_USERNAME')
        self.MORPHL_CASSANDRA_PASSWORD = getenv('MORPHL_CASSANDRA_PASSWORD')
        self.MORPHL_CASSANDRA_KEYSPACE = getenv('MORPHL_CASSANDRA_KEYSPACE')
        self.CASS_REQ_TIMEOUT = 3600.0

        self.auth_provider = PlainTextAuthProvider(
            username=self.MORPHL_CASSANDRA_USERNAME, password=self.MORPHL_CASSANDRA_PASSWORD)
        self.cluster = Cluster(
            contact_points=[self.MORPHL_SERVER_IP_ADDRESS], auth_provider=self.auth_provider)
        self.session = self.cluster.connect(self.MORPHL_CASSANDRA_KEYSPACE)

        self.prepare_statements()

    def prepare_statements(self):
        """
            Prepare statements for database insert queries
        """
        self.prep_stmts = {}

        type_1_list = ['users']
        type_2_list = ['sessions']

        template_for_type_1 = 'INSERT INTO ga_chp_{} (client_id,day_of_data_capture,json_meta,json_data) VALUES (?,?,?,?)'
        template_for_type_2 = 'INSERT INTO ga_chp_{} (client_id,day_of_data_capture,session_id,json_meta,json_data) VALUES (?,?,?,?,?)'

        for report_type in type_1_list:
            self.prep_stmts[report_type] = self.session.prepare(
                template_for_type_1.format(report_type))
        for report_type in type_2_list:
            self.prep_stmts[report_type] = self.session.prepare(
                template_for_type_2.format(report_type))

        self.type_1_set = set(type_1_list)
        self.type_2_set = set(type_2_list)

    def persist_dict_record(self, report_type, meta_dict, data_dict):
        raw_cl_id = data_dict['dimensions'][0]
        client_id = raw_cl_id if raw_cl_id.startswith('GA') else 'UNKNOWN'
        json_meta = dumps(meta_dict)
        json_data = dumps(data_dict)

        if report_type in self.type_1_set:
            bind_list = [client_id, self.DAY_OF_DATA_CAPTURE,
                         json_meta, json_data]
            return {'cassandra_future': self.session.execute_async(self.prep_stmts[report_type],
                                                                   bind_list,
                                                                   timeout=self.CASS_REQ_TIMEOUT),
                    'client_id': client_id}

        if report_type in self.type_2_set:
            session_id = data_dict['dimensions'][1]
            bind_list = [client_id, self.DAY_OF_DATA_CAPTURE,
                         session_id, json_meta, json_data]
            return {'cassandra_future': self.session.execute_async(self.prep_stmts[report_type],
                                                                   bind_list,
                                                                   timeout=self.CASS_REQ_TIMEOUT),
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
        credentials = service_account.Credentials \
                                     .from_service_account_file(self.KEY_FILE_LOCATION) \
                                     .with_scopes(self.SCOPES)
        # Build the service object.
        self.analytics = build('analyticsreporting',
                               'v4', credentials=credentials)

    # Transform list of dimensions names into objects with a 'name' property.
    def format_dimensions(self, dims):
        return [{'name': 'ga:' + dim} for dim in dims]

    # Transform list of metrics names into objects with an 'expression' property.
    def format_metrics(self, metrics):
        return [{'expression': 'ga:' + metric} for metric in metrics]

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
            data_chunk = reports_object.batchGet(
                body={'reportRequests': [query_params]}).execute()
            data_rows = []
            meta_dict = {}
            try:
                data_rows = data_chunk['reports'][0]['data']['rows']
                meta = data_chunk['reports'][0]['columnHeader']
                d_names_list = meta['dimensions']
                m_names_list = [m_meta_dict['name'] for m_meta_dict in meta['metricHeader']['metricHeaderEntries']]
                meta_dict = {'dimensions': d_names_list, 'metrics': m_names_list}
            except Exception as ex:
                print('BEGIN EXCEPTION')
                print(report_type)
                print(exc_info()[0])
                print(str(ex))
                print(dumps(data_chunk['reports'][0]))
                print('END EXCEPTION')
            partial_rl = [self.store.persist_dict_record(
                report_type, meta_dict, data_dict) for data_dict in data_rows]
            complete_responses_list.extend(partial_rl)
            page_token = data_chunk['reports'][0].get('nextPageToken')
            if not page_token:
                break

        # Wait for acks from Cassandra
        [cr['cassandra_future'].result() for cr in complete_responses_list]

        return complete_responses_list

    # Get churned users
    def store_users(self):
        dimensions = ['dimension1', 'deviceCategory']
        metrics = ['sessions', 'sessionDuration', 'entrances',
                   'bounces', 'exits', 'pageValue', 'pageLoadTime', 'pageLoadSample']

        return self.run_report_and_store('users', dimensions, metrics)

    # Get churned users with additional session data
    def store_sessions(self):
        dimensions = ['dimension1', 'dimension2',
                      'sessionCount', 'daysSinceLastSession']
        metrics = ['sessions', 'pageviews', 'uniquePageviews',
                   'screenViews', 'hits', 'timeOnPage']

        return self.run_report_and_store('sessions', dimensions, metrics)

    def run(self):
        self.authenticate()
        self.store_users()
        self.store_sessions()


def main():
    google_analytics = GoogleAnalytics()
    google_analytics.run()


if __name__ == '__main__':
    main()
