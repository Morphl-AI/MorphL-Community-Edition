CREATE KEYSPACE IF NOT EXISTS morphl WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE morphl.ps_area (
  client_id text,
  day_of_data_capture date,
  days_since_last_seen int,
  PRIMARY KEY ((client_id), day_of_data_capture)
) WITH CLUSTERING ORDER BY (day_of_data_capture DESC);

INSERT INTO morphl.ps_area (client_id,day_of_data_capture,days_since_last_seen)
VALUES ('GA1','2018-07-01',15);
INSERT INTO morphl.ps_area (client_id,day_of_data_capture,days_since_last_seen)
VALUES ('GA2','2018-07-03',25);
INSERT INTO morphl.ps_area (client_id,day_of_data_capture,days_since_last_seen)
VALUES ('GA1','2018-07-05',4);
INSERT INTO morphl.ps_area (client_id,day_of_data_capture,days_since_last_seen)
VALUES ('GA2','2018-07-10',6);

