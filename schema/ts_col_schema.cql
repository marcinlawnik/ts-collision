CREATE KEYSPACE IF NOT EXISTS Test
  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

USE Test;

CREATE TABLE TsCol (
  id int,
  col1 int,
  col2 int,
  PRIMARY KEY (id)
);