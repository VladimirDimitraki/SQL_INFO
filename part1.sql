CREATE TABLE IF NOT EXISTS peers (
    nickname VARCHAR(255) PRIMARY KEY,
    birthday DATE
);

CREATE TABLE IF NOT EXISTS tasks(
    title VARCHAR(255) PRIMARY KEY,
    parent_task VARCHAR(255) default NULL,
    max_xp BIGINT
);

CREATE TABLE IF NOT EXISTS checks (
    id BIGINT PRIMARY KEY,
    peer VARCHAR(255),
    task VARCHAR(255),
    date DATE,
    FOREIGN KEY (peer) REFERENCES peers (nickname),
    FOREIGN KEY (task) REFERENCES tasks (title)
);


CREATE TYPE check_status AS ENUM (
  'Start',
  'Success',
  'Failure'
);


CREATE TABLE IF NOT EXISTS time_tracking (
    id BIGINT PRIMARY KEY,
    peer VARCHAR(255),
    date DATE,
    time TIME,
    state BIGINT,
    FOREIGN KEY (peer) REFERENCES peers (nickname)
);


CREATE TABLE IF NOT EXISTS recommendations (
    id BIGINT PRIMARY KEY,
    peer VARCHAR(255),
    recommended_peer VARCHAR(255),
    FOREIGN KEY (peer) REFERENCES peers (nickname),
    FOREIGN KEY (recommended_peer) REFERENCES peers (nickname)
);


CREATE TABLE IF NOT EXISTS friends (
    id BIGINT PRIMARY KEY,
    peer1 VARCHAR(255),
    peer2 VARCHAR(255),
    FOREIGN KEY (peer1) REFERENCES peers (nickname),
    FOREIGN KEY (peer2) REFERENCES peers (nickname)
);


CREATE TABLE IF NOT EXISTS transferred_points (
    id BIGINT PRIMARY KEY,
    peer1 VARCHAR(255),
    peer2 VARCHAR(255),
    points_amount BIGINT,
    FOREIGN KEY (peer1) REFERENCES peers (nickname),
    FOREIGN KEY (peer2) REFERENCES peers (nickname)
);



CREATE TABLE IF NOT EXISTS P2P (
    id BIGINT PRIMARY KEY,
    check_value BIGINT,
    checking_peer VARCHAR(255),
    state check_status,
    time TIME,
    FOREIGN KEY (checking_peer) REFERENCES peers (nickname),
    FOREIGN KEY (check_value) REFERENCES checks (id)
);


CREATE TABLE IF NOT EXISTS verter (
    id BIGINT PRIMARY KEY,
    check_value BIGINT,
    state check_status,
    time TIME,
    FOREIGN KEY (check_value) REFERENCES checks (id)
);

CREATE TABLE IF NOT EXISTS xp (
    id BIGINT PRIMARY KEY,
    check_value BIGINT,
    xp_amount BIGINT,
    FOREIGN KEY (check_value) REFERENCES checks (id)
);



SET datestyle = 'ISO, DMY';


CREATE OR REPLACE PROCEDURE import_data(IN file_name VARCHAR(255), IN table_name VARCHAR(255))
AS $$
BEGIN
    EXECUTE 'COPY ' || table_name || ' FROM ''' || file_name || ''' WITH CSV HEADER DELIMITER '';'';';
END
$$ LANGUAGE plpgsql;

-- INSERT INTO tasks (parent_task) VALUES (NULL);

CALL import_data('/home/administrator/Projects/SQL/SQL2_Info21_v1.0-1/src/dataset_sql/peers.csv', 'peers');

CALL import_data('/home/administrator/Projects/SQL/SQL2_Info21_v1.0-1/src/dataset_sql/tasks.csv', 'tasks');

CALL import_data('/home/administrator/Projects/SQL/SQL2_Info21_v1.0-1/src/dataset_sql/checks.csv', 'checks');

CALL import_data('/home/administrator/Projects/SQL/SQL2_Info21_v1.0-1/src/dataset_sql/friends.csv', 'friends');

CALL import_data('/home/administrator/Projects/SQL/SQL2_Info21_v1.0-1/src/dataset_sql/P2P.csv', 'P2P');

CALL import_data('/home/administrator/Projects/SQL/SQL2_Info21_v1.0-1/src/dataset_sql/recommendations.csv', 'recommendations');

CALL import_data('/home/administrator/Projects/SQL/SQL2_Info21_v1.0-1/src/dataset_sql/time_tracking.csv', 'time_tracking');

CALL import_data('/home/administrator/Projects/SQL/SQL2_Info21_v1.0-1/src/dataset_sql/verter.csv', 'verter');

CALL import_data('/home/administrator/Projects/SQL/SQL2_Info21_v1.0-1/src/dataset_sql/transferred_points.csv', 'transferred_points');

CALL import_data('/home/administrator/Projects/SQL/SQL2_Info21_v1.0-1/src/dataset_sql/xp.csv', 'xp');


CREATE OR REPLACE PROCEDURE export_data(IN export_name VARCHAR(255), IN to_file_name VARCHAR(255))
LANGUAGE plpgsql
AS $$
BEGIN
  EXECUTE format('COPY %I TO %L WITH CSV HEADER', export_name, to_file_name);
END;
$$;

CALL export_data('peers', '/home/administrator/Projects/SQL/SQL2_Info21_v1.0-1/src/data_base/peers.csv');

CALL export_data('tasks', '/home/administrator/Projects/SQL/SQL2_Info21_v1.0-1/src/data_base/tasks.csv');

CALL export_data('checks', '/home/administrator/Projects/SQL/SQL2_Info21_v1.0-1/src/data_base/checks.csv');

CALL export_data('friends', '/home/administrator/Projects/SQL/SQL2_Info21_v1.0-1/src/data_base/friends.csv');

CALL export_data('p2p', '/home/administrator/Projects/SQL/SQL2_Info21_v1.0-1/src/data_base/P2P.csv');

CALL export_data('recommendations', '/home/administrator/Projects/SQL/SQL2_Info21_v1.0-1/src/data_base/recommendations.csv');

CALL export_data('time_tracking', '/home/administrator/Projects/SQL/SQL2_Info21_v1.0-1/src/data_base/time_tracking.csv');

CALL export_data('verter', '/home/administrator/Projects/SQL/SQL2_Info21_v1.0-1/src/data_base/verter.csv');

CALL export_data('verter', '/home/administrator/Projects/SQL/SQL2_Info21_v1.0-1/src/data_base/verter.csv');

CALL export_data('xp', '/home/administrator/Projects/SQL/SQL2_Info21_v1.0-1/src/data_base/xp.csv');