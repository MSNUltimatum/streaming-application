CREATE KEYSPACE botdetector WITH replication = {'class':'SimpleStrategy','replication_factor':1};
CREATE TABLE botdetector.bots
(
    id         text,
    ip         text,
    event_time bigint,
    event_type text,
    url        text,
    is_bot     boolean,
    PRIMARY KEY (id)
);