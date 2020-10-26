CREATE KEYSPACE IF NOT EXISTS Test
  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

USE Test;

CREATE TABLE Nicks (
  nick varchar,
  status varchar,
  PRIMARY KEY (nick)
);

INSERT INTO Nicks (nick) VALUES ('robert');
INSERT INTO Nicks (nick) VALUES ('marcin');
INSERT INTO Nicks (nick) VALUES ('maciej');
INSERT INTO Nicks (nick) VALUES ('filip');
