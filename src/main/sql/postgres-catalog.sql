-- ====================================================================
-- tsdb-sqlcatalog DDL for Postgres
-- Whitehead, 2013
-- jdbc:postgresql://localhost:5432/opentsdb (tsdb/tsdb)
-- ====================================================================

-- ===========================================================================================
--  The Sequence driving the synthetic PKs
-- ===========================================================================================
CREATE SEQUENCE FQN_SEQ START WITH 1 INCREMENT BY 20;
CREATE SEQUENCE FQN_TP_SEQ START WITH 1 INCREMENT BY 100;
CREATE SEQUENCE TAGK_SEQ START WITH 1 INCREMENT BY 20;
CREATE SEQUENCE TAGV_SEQ START WITH 1 INCREMENT BY 20;
CREATE SEQUENCE METRIC_SEQ START WITH 1 INCREMENT BY 20;
CREATE SEQUENCE TAGPAIR_SEQ START WITH 1 INCREMENT BY 20;
CREATE SEQUENCE ANN_SEQ START WITH 1 INCREMENT BY 20;
CREATE SEQUENCE QID_SEQ START WITH 1 INCREMENT BY 20;

-- =================================================================
-- TAG KEYS
-- =================================================================

CREATE TABLE TSD_TAGK (
    TAGK_ID NUMERIC NOT NULL ,
    VERSION INT NOT NULL,
    NAME VARCHAR(200) NOT NULL,
    SORT INT,
    CREATED TIMESTAMP DEFAULT current_timestamp NOT NULL,
    LAST_UPDATE TIMESTAMP DEFAULT current_timestamp NOT NULL,
    DESCRIPTION VARCHAR(120) ,
    DISPLAY_NAME VARCHAR(60),
    NOTES VARCHAR(120),
    CUSTOM JSONB     
); 



COMMENT ON TABLE TSD_TAGK IS 'Table storing distinct time-series tag keys';
COMMENT ON COLUMN TSD_TAGK.TAGK_ID IS 'The tag key identifier';
COMMENT ON COLUMN TSD_TAGK.VERSION IS 'The version of this instance';
COMMENT ON COLUMN TSD_TAGK.NAME IS 'The tag key';
COMMENT ON COLUMN TSD_TAGK.CREATED IS 'The timestamp of the creation of the UID';
COMMENT ON COLUMN TSD_TAGK.LAST_UPDATE IS 'The timestamp of the last update to this TAGK';
COMMENT ON COLUMN TSD_TAGK.DESCRIPTION IS 'An optional description for this tag key';
COMMENT ON COLUMN TSD_TAGK.DISPLAY_NAME IS 'An optional display name for this tag key';
COMMENT ON COLUMN TSD_TAGK.NOTES IS 'Optional notes for this tag key';
COMMENT ON COLUMN TSD_TAGK.CUSTOM IS 'An optional map of key/value pairs encoded in JSON for this tag key';

CREATE UNIQUE INDEX TSD_TAGK_AK ON TSD_TAGK (NAME);
ALTER TABLE TSD_TAGK ADD CONSTRAINT TSD_TAGK_PK PRIMARY KEY ( TAGK_ID ) ;

-- =================================================================
-- TAG VALUES
-- =================================================================

CREATE TABLE TSD_TAGV (
    TAGV_ID NUMERIC NOT NULL ,
    VERSION INT NOT NULL,
    NAME VARCHAR(200) NOT NULL,
    CREATED TIMESTAMP DEFAULT current_timestamp NOT NULL,
    LAST_UPDATE TIMESTAMP DEFAULT current_timestamp NOT NULL,
    DESCRIPTION VARCHAR(120) ,
    DISPLAY_NAME VARCHAR(60),
    NOTES VARCHAR(120),
    CUSTOM JSONB NOT NULL DEFAULT '{}'
); 
COMMENT ON TABLE TSD_TAGV IS 'Table storing distinct time-series tag values';
COMMENT ON COLUMN TSD_TAGV.TAGV_ID IS 'The tag value identfier';
COMMENT ON COLUMN TSD_TAGV.VERSION IS 'The version of this instance';
COMMENT ON COLUMN TSD_TAGV.NAME IS 'The tag value';
COMMENT ON COLUMN TSD_TAGV.CREATED IS 'The timestamp of the creation of the UID';
COMMENT ON COLUMN TSD_TAGV.LAST_UPDATE IS 'The timestamp of the last update to this TAGV';
COMMENT ON COLUMN TSD_TAGV.DESCRIPTION IS 'An optional description for this tag value';
COMMENT ON COLUMN TSD_TAGV.DISPLAY_NAME IS 'An optional display name for this tag value';
COMMENT ON COLUMN TSD_TAGV.NOTES IS 'Optional notes for this tag value';
COMMENT ON COLUMN TSD_TAGV.CUSTOM IS 'An optional map of key/value pairs encoded in JSON for this tag value';

CREATE UNIQUE INDEX TSD_TAGV_AK ON TSD_TAGV (NAME);
ALTER TABLE TSD_TAGV ADD CONSTRAINT TSD_TAGV_PK PRIMARY KEY ( TAGV_ID ) ;

-- =================================================================
-- METRICS
-- =================================================================

CREATE TABLE TSD_METRIC (
    METRIC_ID NUMERIC NOT NULL,
    VERSION INT NOT NULL,
    NAME VARCHAR(200) NOT NULL,
    CREATED TIMESTAMP DEFAULT current_timestamp NOT NULL,
    LAST_UPDATE TIMESTAMP DEFAULT current_timestamp NOT NULL,
    DESCRIPTION VARCHAR(120) ,
    DISPLAY_NAME VARCHAR(60),
    NOTES VARCHAR(120),
    CUSTOM JSONB NOT NULL DEFAULT '{}'
); 
COMMENT ON TABLE TSD_METRIC IS 'Table storing distinct time-series metric names';
COMMENT ON COLUMN TSD_METRIC.METRIC_ID IS 'The metric identifier';
COMMENT ON COLUMN TSD_METRIC.VERSION IS 'The version of this instance';
COMMENT ON COLUMN TSD_METRIC.NAME IS 'The metric name';
COMMENT ON COLUMN TSD_METRIC.CREATED IS 'The timestamp of the creation of the metric';
COMMENT ON COLUMN TSD_METRIC.LAST_UPDATE IS 'The timestamp of the last update to this metric';
COMMENT ON COLUMN TSD_METRIC.DESCRIPTION IS 'An optional description for this metric name';
COMMENT ON COLUMN TSD_METRIC.DISPLAY_NAME IS 'An optional display name for this metric name';
COMMENT ON COLUMN TSD_METRIC.NOTES IS 'Optional notes for this metric name';
COMMENT ON COLUMN TSD_METRIC.CUSTOM IS 'An optional map of key/value pairs encoded in JSON for this metric name';

CREATE UNIQUE INDEX TSD_METRIC_AK ON TSD_METRIC (NAME);
ALTER TABLE TSD_METRIC ADD CONSTRAINT TSD_METRIC_PK PRIMARY KEY ( METRIC_ID );

-- =================================================================
-- ASSOCIATIVES
-- =================================================================


CREATE TABLE TSD_TAGPAIR (
    TAGPAIR_ID NUMERIC NOT NULL,
    TAGK_ID NUMERIC NOT NULL REFERENCES TSD_TAGK(TAGK_ID),
    TAGV_ID NUMERIC NOT NULL REFERENCES TSD_TAGV(TAGV_ID)
); 

COMMENT ON TABLE TSD_TAGPAIR IS 'Table storing the observed unique tag key and value pairs associated with a time-series/TSMeta';
COMMENT ON COLUMN TSD_TAGPAIR.TAGPAIR_ID IS 'The unique identifier of a tag pair';
COMMENT ON COLUMN TSD_TAGPAIR.TAGK_ID IS 'The pair tag key id';
COMMENT ON COLUMN TSD_TAGPAIR.TAGV_ID IS 'The pair tag value id';

CREATE UNIQUE INDEX TSD_TAGPAIR_AK ON TSD_TAGPAIR (TAGK_ID ASC, TAGV_ID ASC);
CREATE INDEX TSD_TAGPAIR_K_IDX ON TSD_TAGPAIR (TAGK_ID ASC);
CREATE INDEX TSD_TAGPAIR_V_IDX ON TSD_TAGPAIR (TAGV_ID ASC);
ALTER TABLE TSD_TAGPAIR ADD CONSTRAINT TSD_TAGPAIR_PK PRIMARY KEY ( TAGPAIR_ID ) ;

-- =================================================================

CREATE TABLE TSD_FQN_TAGPAIR (
	FQN_TP_ID NUMERIC NOT NULL,
  TAGS SMALLINT NOT NULL,
	TAGPAIR_IDS NUMERIC[] NOT NULL
); 

COMMENT ON TABLE TSD_FQN_TAGPAIR IS 'Associative table between TSD_TSMETA and TSD_TAGPAIR, or the TSMeta and the Tag keys and values of the UIDMetas therein';
COMMENT ON COLUMN TSD_FQN_TAGPAIR.FQN_TP_ID IS 'Synthetic primary key of an association between an FQN and a Tag Pair';
COMMENT ON COLUMN TSD_FQN_TAGPAIR.TAGS IS 'The number of tags';
COMMENT ON COLUMN TSD_FQN_TAGPAIR.TAGPAIR_IDS IS 'An array of the tagpairs, one ID for each child tag key/value pair';


ALTER TABLE TSD_FQN_TAGPAIR ADD CONSTRAINT TSD_FQN_TAGPAIR_PK PRIMARY KEY ( FQN_TP_ID ) ;
CREATE UNIQUE INDEX TSD_FQN_TAGPAIRS_AK ON TSD_FQN_TAGPAIR (TAGPAIR_IDS);

-- =================================================================

CREATE TABLE TSD_TAGPAIR_ASSOC (
  FQN_TP_ID NUMERIC NOT NULL,
  TAGPAIR_ID NUMERIC NOT NULL
);

COMMENT ON TABLE TSD_TAGPAIR_ASSOC IS 'Additional associative table between TSD_TAGPAIR and TSD_TAGPAIR to support more optimized joins';

ALTER TABLE TSD_TAGPAIR_ASSOC ADD CONSTRAINT TSD_TAGPAIR_ASSOC_PK PRIMARY KEY ( FQN_TP_ID, TAGPAIR_ID );
ALTER TABLE TSD_TAGPAIR_ASSOC ADD CONSTRAINT TSD_TAGPAIR_ASSOC_FQN_FK FOREIGN KEY(FQN_TP_ID) REFERENCES TSD_FQN_TAGPAIR ( FQN_TP_ID );
ALTER TABLE TSD_TAGPAIR_ASSOC ADD CONSTRAINT TSD_TAGPAIR_ASSOC_TAGPAIR_FK FOREIGN KEY(TAGPAIR_ID) REFERENCES TSD_TAGPAIR ( TAGPAIR_ID );

-- =================================================================
-- TSMETAS
-- =================================================================

CREATE TABLE TSD_TSMETA (
	FQNID NUMERIC NOT NULL,
	VERSION INT NOT NULL,
	METRIC_ID NUMERIC NOT NULL,
  FQN_TP_ID NUMERIC NOT NULL,
	FQN VARCHAR(4000) NOT NULL,
  CREATED TIMESTAMP DEFAULT current_timestamp NOT NULL,
  LAST_UPDATE TIMESTAMP DEFAULT current_timestamp NOT NULL,
	MAX_VALUE NUMERIC,
	MIN_VALUE NUMERIC,
	DATA_TYPE VARCHAR(20),
	DESCRIPTION VARCHAR(60),
	DISPLAY_NAME VARCHAR(60),
	NOTES VARCHAR(120),
	UNITS VARCHAR(20),
	RETENTION INTEGER DEFAULT 0,
	CUSTOM JSONB NOT NULL DEFAULT '{}'
); 


COMMENT ON TABLE TSD_TSMETA IS 'Table storing each distinct time-series TSMeta and its attributes';
COMMENT ON COLUMN TSD_TSMETA.FQNID IS 'A synthetic unique identifier for each individual TSMeta/TimeSeries entry';
COMMENT ON COLUMN TSD_TSMETA.VERSION IS 'The version of this instance';
COMMENT ON COLUMN TSD_TSMETA.METRIC_ID IS 'The unique identifier of the metric name associated with this TSMeta';
COMMENT ON COLUMN TSD_TSMETA.FQN_TP_ID IS 'The unique identifier of the tag set associated with this TSMeta';
COMMENT ON COLUMN TSD_TSMETA.FQN IS 'The fully qualified metric name';
COMMENT ON COLUMN TSD_TSMETA.CREATED IS 'The timestamp of the creation of the TSMeta';
COMMENT ON COLUMN TSD_METRIC.LAST_UPDATE IS 'The timestamp of the last update to this TSMeta';
COMMENT ON COLUMN TSD_TSMETA.MAX_VALUE IS 'Optional max value for the timeseries';
COMMENT ON COLUMN TSD_TSMETA.MIN_VALUE IS 'Optional min value for the timeseries';
COMMENT ON COLUMN TSD_TSMETA.DATA_TYPE IS 'An optional and arbitrary data type designation for the time series, e.g. COUNTER or GAUGE';
COMMENT ON COLUMN TSD_TSMETA.DESCRIPTION IS 'An optional description for the time-series';
COMMENT ON COLUMN TSD_TSMETA.DISPLAY_NAME IS 'An optional name for the time-series';
COMMENT ON COLUMN TSD_TSMETA.NOTES IS 'Optional notes for the time-series';
COMMENT ON COLUMN TSD_TSMETA.UNITS IS 'Optional units designation for the time-series';
COMMENT ON COLUMN TSD_TSMETA.RETENTION IS 'Optional retention time for the time-series in days where 0 is indefinite';
COMMENT ON COLUMN TSD_TSMETA.CUSTOM IS 'An optional map of key/value pairs encoded in JSON for this TSMeta';

ALTER TABLE TSD_TSMETA ADD CONSTRAINT TSD_FQN_PK PRIMARY KEY ( FQNID ) ;
CREATE UNIQUE INDEX TSD_FQN_FQN_AK ON TSD_TSMETA (FQN);

ALTER TABLE TSD_TSMETA ADD CONSTRAINT TSD_FQN_METRIC_FK FOREIGN KEY(METRIC_ID) REFERENCES TSD_METRIC ( METRIC_ID );
ALTER TABLE TSD_TSMETA ADD CONSTRAINT TSD_FQN_TAGS_FK FOREIGN KEY(FQN_TP_ID) REFERENCES TSD_FQN_TAGPAIR ( FQN_TP_ID );


-- =================================================================
-- ANNOTATIONS
-- =================================================================


CREATE TABLE TSD_ANNOTATION (
	ANNID NUMERIC NOT NULL,
	VERSION INT NOT NULL,
	START_TIME TIMESTAMP NOT NULL,
    LAST_UPDATE TIMESTAMP DEFAULT current_timestamp NOT NULL,
	DESCRIPTION VARCHAR(120) NOT NULL,
    NOTES VARCHAR(120),
	FQNID NUMERIC,
    END_TIME TIMESTAMP,
    CUSTOM JSONB NOT NULL DEFAULT '{}'
); 
COMMENT ON TABLE TSD_ANNOTATION IS 'Table storing created annotations';
COMMENT ON COLUMN TSD_ANNOTATION.ANNID IS 'The synthetic unique identifier for this annotation';
COMMENT ON COLUMN TSD_ANNOTATION.VERSION IS 'The version of this instance';
COMMENT ON COLUMN TSD_ANNOTATION.START_TIME IS 'The effective start time for this annotation';
COMMENT ON COLUMN TSD_ANNOTATION.LAST_UPDATE IS 'The timestamp of the last update to this Annotation';
COMMENT ON COLUMN TSD_ANNOTATION.DESCRIPTION IS 'The mandatory description for this annotation';
COMMENT ON COLUMN TSD_ANNOTATION.NOTES IS 'Optional notes for this annotation';
COMMENT ON COLUMN TSD_ANNOTATION.FQNID IS 'An optional reference to the associated TSMeta. If null, this will be a global annotation';
COMMENT ON COLUMN TSD_ANNOTATION.END_TIME IS 'The optional effective end time for this annotation';
COMMENT ON COLUMN TSD_ANNOTATION.CUSTOM IS 'An optional map of key/value pairs encoded in JSON for this annotation';

ALTER TABLE TSD_ANNOTATION ADD CONSTRAINT TSD_ANNOTATION_PK PRIMARY KEY ( ANNID ) ;
CREATE UNIQUE INDEX TSD_ANNOTATION_AK ON TSD_ANNOTATION (START_TIME, FQNID);
ALTER TABLE TSD_ANNOTATION ADD CONSTRAINT TSD_ANNOTATION_FQNID_FK FOREIGN KEY(FQNID) REFERENCES TSD_TSMETA ( FQNID ) ON DELETE CASCADE;

ALTER TABLE TSD_FQN_TAGPAIR ADD CONSTRAINT TSD_FQN_TAGPAIR_FQNID_FK FOREIGN KEY(FQNID) REFERENCES TSD_TSMETA ( FQNID ) ON DELETE CASCADE;


-- ==============================================================================================
--   UPDATE TRIGGERS
-- ==============================================================================================


CREATE OR REPLACE FUNCTION TSD_X_UPDATED_TRG() RETURNS trigger AS $TSD_X_UPDATED_TRG$
    BEGIN
	NEW.VERSION := NEW.VERSION +1;
	NEW.LAST_UPDATE := current_timestamp;
	RETURN NEW;
    END;
$TSD_X_UPDATED_TRG$ LANGUAGE plpgsql;


CREATE TRIGGER "TSD_TAGK_UPDATED_TRG" BEFORE UPDATE ON tsd_tagk FOR EACH ROW EXECUTE PROCEDURE tsd_x_updated_trg();
CREATE TRIGGER "TSD_TAGV_UPDATED_TRG" BEFORE UPDATE ON tsd_tagv FOR EACH ROW EXECUTE PROCEDURE tsd_x_updated_trg();
CREATE TRIGGER "TSD_METRIC_UPDATED_TRG" BEFORE UPDATE ON tsd_metric FOR EACH ROW EXECUTE PROCEDURE tsd_x_updated_trg();
CREATE TRIGGER "TSD_TSMETA_UPDATED_TRG" BEFORE UPDATE ON tsd_tsmeta FOR EACH ROW EXECUTE PROCEDURE tsd_x_updated_trg();
CREATE TRIGGER "TSD_ANNOTATION_UPDATED_TRG" BEFORE UPDATE ON tsd_annotation FOR EACH ROW EXECUTE PROCEDURE tsd_x_updated_trg

-- ==============================================================================================
--   TIME SERIES DATA TABLES
-- ==============================================================================================


CREATE TABLE TSDB (
  time        TIMESTAMP         NOT NULL,
  fqnid      NUMERIC           NOT NULL,
  value       NUMERIC           NOT NULL  
);

ALTER TABLE TSDB ADD CONSTRAINT TSDB_TSMETA_FK FOREIGN KEY(FQNID) REFERENCES TSD_TSMETA ( FQNID );
SELECT create_hypertable('tsdb', 'time', 'fqnid', 2);
CREATE UNIQUE INDEX TSDB_TIME_FQN_IDX ON tsdb (fqnid, time DESC);

-- ==============================================================================================
--   VIEW
-- ==============================================================================================



CREATE OR REPLACE VIEW RC AS
SELECT 'TSD_TSMETA' as "TABLE", COUNT(*) as "ROW COUNT" FROM TSD_TSMETA
UNION      
SELECT 'TSD_TAGK', COUNT(*) FROM TSD_TAGK
UNION      
SELECT 'TSD_TAGV', COUNT(*) FROM TSD_TAGV
UNION      
SELECT 'TSD_METRIC', COUNT(*) FROM TSD_METRIC
UNION      
SELECT 'TSD_TAGPAIR', COUNT(*) FROM TSD_TAGPAIR
UNION      
SELECT 'TSD_FQN_TAGPAIR', COUNT(*) FROM TSD_FQN_TAGPAIR
UNION
SELECT 'TSD_TAGPAIR_ASSOC', COUNT(*) FROM TSD_TAGPAIR_ASSOC
UNION
SELECT 'TSD_ANNOTATION', COUNT(*) FROM TSD_ANNOTATION
UNION
SELECT 'TSDB', COUNT(*) FROM TSDB
ORDER BY 2 DESC;

CREATE OR REPLACE FUNCTION clean()
  RETURNS void AS
$BODY$
BEGIN
TRUNCATE TABLE TSD_FQN_TAGPAIR CASCADE;
TRUNCATE TABLE TSD_TAGPAIR_ASSOC CASCADE;
TRUNCATE TABLE TSD_TAGPAIR CASCADE;
TRUNCATE TABLE TSD_METRIC CASCADE;
TRUNCATE TABLE TSD_TAGV CASCADE;
TRUNCATE TABLE TSD_TAGK CASCADE;
TRUNCATE TABLE TSD_TSMETA CASCADE;
TRUNCATE TABLE TSD_ANNOTATION CASCADE;
TRUNCATE TABLE TSDB CASCADE;
PERFORM TAGK('dc', 0);
PERFORM TAGK('host', 1);
PERFORM TAGK('app', 2);
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;


CREATE VIEW VERSIONS AS
SELECT 'TSMETA', version, count(*) CNT FROM TSD_TSMETA group by version
UNION ALL
SELECT 'TAGK', version, count(*) CNT FROM TSD_TAGK group by version
UNION ALL
SELECT 'TAGV', version, count(*) CNT FROM TSD_TAGV group by version
UNION ALL
SELECT 'METRIC', version, count(*) CNT FROM TSD_METRIC group by version
UNION ALL
SELECT 'ANNOTATION', version, count(*) CNT FROM TSD_ANNOTATION group by version
ORDER BY 1,2,3;

CREATE VIEW TSD_TAGS AS 
SELECT K.NAME K, V.NAME V, T.TAGK_ID, T.TAGV_ID, T.TAGPAIR_ID
FROM TSD_TAGV V, TSD_TAGK K, TSD_TAGPAIR T
WHERE V.TAGV_ID = T.TAGV_ID
AND K.TAGK_ID = T.TAGK_ID;

CREATE FUNCTION TAGK(value text) RETURNS NUMERIC AS  $D$  
DECLARE
    ID NUMERIC;
BEGIN   
  SELECT TAGK_ID INTO ID FROM TSD_TAGK WHERE NAME = value;
  IF NOT FOUND THEN  
    SELECT NEXTVAL('TAGK_SEQ') INTO ID;
    INSERT INTO TSD_TAGK (TAGK_ID, VERSION, NAME) VALUES
    (ID, 1, value);
  END IF;
  RETURN ID;
END;   
$D$
LANGUAGE PLPGSQL;


CREATE FUNCTION TAGK(value text, sort int) RETURNS NUMERIC AS  $D$  
DECLARE
    ID NUMERIC;
BEGIN   
  SELECT TAGK_ID INTO ID FROM TSD_TAGK WHERE NAME = value;
  IF NOT FOUND THEN  
    SELECT NEXTVAL('TAGK_SEQ') INTO ID;
    INSERT INTO TSD_TAGK (TAGK_ID, VERSION, NAME, SORT) VALUES
    (ID, 1, value, sort);
  END IF;
  RETURN ID;
END;   
$D$
LANGUAGE PLPGSQL;

CREATE FUNCTION TAGV(value text) RETURNS NUMERIC AS  $D$  
DECLARE
    ID NUMERIC;
BEGIN   
  SELECT TAGV_ID INTO ID FROM TSD_TAGV WHERE NAME = value;
  IF NOT FOUND THEN  
    SELECT NEXTVAL('TAGV_SEQ') INTO ID;
    INSERT INTO TSD_TAGV (TAGV_ID, VERSION, NAME) VALUES
    (ID, 1, value);
  END IF;
  RETURN ID;
END;   
$D$
LANGUAGE PLPGSQL;

CREATE FUNCTION METRIC(value text) RETURNS NUMERIC AS  $D$  
DECLARE
    ID NUMERIC;
BEGIN   
  SELECT METRIC_ID INTO ID FROM TSD_METRIC WHERE NAME = value;
  IF NOT FOUND THEN  
    SELECT NEXTVAL('METRIC_SEQ') INTO ID;
    INSERT INTO TSD_METRIC (METRIC_ID, VERSION, NAME) VALUES
    (ID, 1, value);
  END IF;
  RETURN ID;
END;   
$D$SELECT * FROM RC
LANGUAGE PLPGSQL;

CREATE FUNCTION TAGKNAME(id numeric) RETURNS TEXT AS  $D$  
DECLARE
    N TEXT;
BEGIN   
  SELECT NAME INTO N FROM TSD_TAGK WHERE TAGK_ID = id;
  IF NOT FOUND THEN  
  RAISE 'No TAGK for id %', id;
  END IF;
  RETURN N;
END;   
$D$
LANGUAGE PLPGSQL;

CREATE FUNCTION TAGVNAME(id numeric) RETURNS TEXT AS  $D$  
DECLARE
    N TEXT;
BEGIN   
  SELECT NAME INTO N FROM TSD_TAGV WHERE TAGV_ID = id;
  IF NOT FOUND THEN  
  RAISE 'No TAGV for id %', id;
  END IF;
  RETURN N;
END;   
$D$
LANGUAGE PLPGSQL;

CREATE FUNCTION METRICNAME(id numeric) RETURNS TEXT AS  $D$  
DECLARE
    N TEXT;
BEGIN   
  SELECT NAME INTO N FROM TSD_METRIC WHERE METRIC_ID = id;
  IF NOT FOUND THEN  
  RAISE 'No METRIC for id %', id;
  END IF;
  RETURN N;
END;   
$D$
LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION metric_ids(metricName text)
  RETURNS NUMERIC[] AS
$BODY$
DECLARE
  metric_ids_arr NUMERIC[];    
BEGIN
  select array_agg(metric_id) into metric_ids_arr from tsd_metric where name like replace(metricName, '*', '%');
  return metric_ids_arr;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;



CREATE OR REPLACE FUNCTION TAGPAIRID(id numeric) RETURNS TEXT[] AS  $D$  
DECLARE
    KV TEXT[] = ARRAY['', ''];
BEGIN   
  SELECT array[TAGKNAME(TAGK_ID), TAGVNAME(TAGV_ID)] INTO KV FROM TSD_TAGPAIR 
  WHERE TAGPAIR_ID = id;
  RETURN KV;
END;   
$D$
LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION resolveTags(fqn_tagpair_id numeric)
  RETURNS JSONB AS
$BODY$
DECLARE
  KEYS TEXT[];
  VALUES TEXT[];
  TPIDS NUMERIC[];
BEGIN
  SELECT tagpair_ids INTO TPIDS FROM TSD_FQN_TAGPAIR WHERE fqn_tp_id = fqn_tagpair_id;

  SELECT array_agg(NAME) 
  INTO KEYS
  FROM TSD_TAGK K, TSD_TAGPAIR T
  WHERE K.TAGK_ID  = T.TAGK_ID
  AND T.TAGPAIR_ID = ANY(TPIDS);

  SELECT array_agg(NAME) 
  INTO VALUES
  FROM TSD_TAGV V, TSD_TAGPAIR T
  WHERE V.TAGV_ID  = T.TAGV_ID
  AND T.TAGPAIR_ID = ANY(TPIDS);

  RETURN jsonb_object(KEYS, VALUES);
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

CREATE OR REPLACE FUNCTION fqnid_to_jsonb(id numeric)
  RETURNS JSONB AS
$BODY$
DECLARE
  MNAME TEXT = '';
  TPID NUMERIC;
  MOBJ JSONB;
BEGIN
  SELECT fqn_tp_id INTO TPID 
  FROM TSD_FQN_TAGPAIR
  WHERE FQNID = id;
  --
  SELECT NAME INTO MNAME
  FROM TSD_METRIC M, TSD_TSMETA T
  WHERE M.METRIC_ID = T.METRIC_ID
  AND T.FQNID = id;
  --
  return jsonb_build_object('tags', resolveTags(TPID), 'metric', MNAME);
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

CREATE OR REPLACE FUNCTION fqnid_to_jsonstr(id numeric)
  RETURNS TEXT AS
$BODY$
BEGIN
  return array_to_json(array[fqnid_to_jsonb(id)])->>0;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

CREATE OR REPLACE FUNCTION fqnid_to_str(id numeric)
  RETURNS TEXT AS
$BODY$
DECLARE
  MNAME TEXT = '';
  MOBJ JSONB;
  TAG RECORD;
BEGIN
  MOBJ = fqnid_to_jsonb(id);
  MNAME = MOBJ->>'metric';
  MNAME = MNAME || ':';
  FOR TAG IN SELECT * FROM jsonb_each_text(MOBJ->'tags') LOOP
    MNAME = MNAME || TAG.key || '=' || TAG.value || ',';
  END LOOP;
  
  RETURN trim(trailing ',' from MNAME);
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;


CREATE FUNCTION TAGPAIR(key text, value text) RETURNS NUMERIC AS  $D$  
DECLARE
    ID NUMERIC;
    K NUMERIC;
    V NUMERIC;
BEGIN   
  K = TAGK(key);
  V = TAGV(value);
  SELECT TAGPAIR_ID INTO ID FROM TSD_TAGPAIR 
  WHERE TAGK_ID = K
  AND TAGV_ID = V;
  IF NOT FOUND THEN  
    SELECT NEXTVAL('TAGPAIR_SEQ') INTO ID;
    INSERT INTO TSD_TAGPAIR (TAGPAIR_ID, TAGK_ID, TAGV_ID) VALUES
    (ID, K, V);
  END IF;
  RETURN ID;
END;   
$D$
LANGUAGE PLPGSQL;

--select tsd_id(jsonb('{"metric": "sys.cpu", "tagsx": { "dc": "lga", "host": "web01", "app" : "edge-web" }}'))


CREATE OR REPLACE FUNCTION flatten(metric jsonb)
  RETURNS text AS
$BODY$
DECLARE
    FLAT TEXT;
    TAGREC record;
    M TEXT = '';
    TAGS JSON;
BEGIN
    M = metric->>'metric';
    FLAT = M || ':';
    TAGS = metric->'tags';
    FOR TAGREC IN SELECT J.KEY, J.VALUE 
    FROM json_each_text(TAGS) J, TSD_TAGK K
    WHERE J.KEY = K.NAME
    ORDER  BY K.SORT, K.NAME
    LOOP
        FLAT = FLAT || TAGREC.KEY || '=' || TAGREC.VALUE || ',';
    END LOOP;
    RETURN trim(trailing ',' from FLAT);
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

CREATE OR REPLACE FUNCTION flatten(metric text)
  RETURNS text AS
$BODY$
BEGIN
    RETURN flatten(jsonb(metric));
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

CREATE OR REPLACE FUNCTION FQNTAGPAIRID_ARR(id numeric) RETURNS TEXT[][] AS  $D$  
DECLARE
    KV TEXT[][];
BEGIN   
  select array_agg(tagpairid(X)) INTO KV from (select unnest(tagpair_ids) as X from tsd_fqn_tagpair where fqn_tp_id = id) as Y;
  RETURN KV;
END;   
$D$
LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION FQNTAGPAIRID_STR(id numeric) RETURNS TEXT AS  $D$  
DECLARE
    KVS TEXT;
BEGIN   
  select array_to_string(array_agg(array_to_string(tagpairid(X), '=')), ',') INTO KVS from (select unnest(tagpair_ids) as X from tsd_fqn_tagpair where fqn_tp_id = id) as Y;
  RETURN KVS;
END;   
$D$
LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION tsd_id(metricjson jsonb)
  RETURNS JSON AS
$BODY$
DECLARE
    actual JSONB;
    jtype TEXT;
    metric JSONB;
    TAGS JSON;
    TAGPAIRS NUMERIC[];
    TAGPAIR NUMERIC;
    METRICID NUMERIC;
    TAGCOUNT INT;
    TAGREC record;
    MREC RECORD;
    FQN_TAGPAIR_ID NUMERIC;
    FQN_ID NUMERIC;
    FQN TEXT;
    TS TIMESTAMP;
    TSSTR TEXT;
    TSSTRSECS TEXT;
    TSLEN SMALLINT;
    TSNUM NUMERIC;
    IDS NUMERIC[] = ARRAY[]::integer[];
BEGIN
    jtype = jsonb_typeof(metricjson);
    IF(jtype='object') THEN
  actual = jsonb_build_array(metricjson);
    ELSEIF(jtype='array') THEN
  actual = metricjson;
    ELSE
  RAISE 'Invalid Metric JSON: %. Not array or object', metricjson;
    END IF;
    -- 
    FOR MREC IN SELECT * FROM jsonb_array_elements(actual) LOOP
      TAGPAIRS = ARRAY[]::NUMERIC[];
      metric = MREC.value;
      IF(metric ?& array['metric','tags']) THEN
    -- NOTHING
      ELSE
    IDS = IDS || -1;
    RAISE NOTICE 'Invalid Metric JSON: %. Missing metric or tags', metric;
      END IF;
      TAGS = metric->'tags';
      SELECT COUNT(*) INTO TAGCOUNT FROM (SELECT * FROM json_object_keys(TAGS)) X;
      IF(TAGCOUNT=0 OR TAGCOUNT > 8) THEN 
    IDS = IDS || -1;
    RAISE NOTICE 'Invalid Metric JSON: %. Invalid Tag Count: %', metric, TAGCOUNT; 
      END IF;
      FOR TAGREC IN SELECT J.KEY, J.VALUE 
      FROM json_each_text(TAGS) J
      LOOP
  PERFORM TAGK(TAGREC.KEY);
  PERFORM TAGV(TAGREC.VALUE);
      END LOOP;
      
      FOR TAGREC IN SELECT J.KEY, J.VALUE 
      FROM json_each_text(TAGS) J, TSD_TAGK K
      WHERE J.KEY = K.NAME
      ORDER  BY K.SORT, K.NAME

      LOOP   
  TAGPAIR = tagpair(TAGREC.KEY, TAGREC.VALUE);
  --RAISE NOTICE 'Ordered Tag Pair: %=%', TAGREC.KEY, TAGREC.VALUE;
  TAGPAIRS = TAGPAIRS || TAGPAIR;
      END LOOP;
      --RAISE NOTICE 'METRIC: %', metric->>'metric';
      --RAISE NOTICE 'TAGPAIRS: %', TAGPAIRS;
      METRICID = METRIC(metric->>'metric');
      SELECT T.FQNID INTO FQN_ID 
      FROM TSD_FQN_TAGPAIR P, TSD_TSMETA T
      WHERE P.FQNID = T.FQNID
      AND T.METRIC_ID = METRICID
      AND TAGPAIR_IDS = TAGPAIRS;
      IF NOT FOUND THEN  
    SELECT NEXTVAL('FQN_TP_SEQ') INTO FQN_TAGPAIR_ID;
    SELECT NEXTVAL('FQN_SEQ') INTO FQN_ID;
    INSERT INTO TSD_TSMETA (FQNID, VERSION, METRIC_ID, FQN)
    VALUES ( FQN_ID, 1, METRICID, flatten(metric));
    INSERT INTO TSD_FQN_TAGPAIR (FQN_TP_ID, FQNID, TAGPAIR_IDS, TATAGS)
    VALUES (FQN_TAGPAIR_ID, FQN_ID, TAGPAIRS, TAGCOUNT);
      END IF;
    -- =================================================================================
    --  Value Processing
    -- =================================================================================
      IF(metric ?& array['value']) THEN
    TSSTR = trim(metric->>'timestamp');
    IF(TSSTR IS NULL) THEN
      IF(metric ?& array['timesecs']) THEN
        TSSTR = trim(metric->>'timesecs');
        TSNUM = CAST(TSSTR as NUMERIC);
        TS = to_timestamp(TSNUM);
      ELSE
        TS = now();
      END IF;
    ELSE
      TSNUM = CAST(TSSTR as NUMERIC);
      TS = to_timestamp(TSNUM::double precision / 1000);
    END IF;
    BEGIN
  INSERT INTO TSDB VALUES(TS, FQN_ID, CAST(metric->>'value' as NUMERIC));
  EXCEPTION
    WHEN unique_violation THEN
      perform pg_notify('ERRORS', 'UNIQ VIOL ON TSDB [' || TS || ',' || FQN_ID || ']');
    END;
    END IF;
      IDS = IDS || FQN_ID;
    END LOOP;
    -- =================================================================================
    RETURN to_json(IDS);
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

CREATE OR REPLACE FUNCTION tsd_id_for_ids(metricjson jsonb)
  RETURNS JSONB AS
$BODY$
DECLARE
    actual JSONB;
    jtype TEXT;
    metric JSONB;
    TAGS JSON;
    TAGPAIRS NUMERIC[];
    TAGPAIR NUMERIC;
    METRICID NUMERIC;
    TAGCOUNT INT;
    TAGREC record;
    MREC RECORD;
    FQN_TAGPAIR_ID NUMERIC;
    FQN_ID NUMERIC;
    FQN TEXT;
    TS TIMESTAMP;
    TSSTR TEXT;
    TSSTRSECS TEXT;
    TSLEN SMALLINT;
    TSNUM NUMERIC;
    IDMAP JSONB  = jsonb_build_object();
    FLATMETRIC TEXT[];
BEGIN
    jtype = jsonb_typeof(metricjson);
    IF(jtype='object') THEN
  actual = jsonb_build_array(metricjson);
    ELSEIF(jtype='array') THEN
  actual = metricjson;
    ELSE
  RAISE 'Invalid Metric JSON: %. Not array or object', metricjson;
    END IF;
    -- 
    FOR MREC IN SELECT * FROM jsonb_array_elements(actual) LOOP
      TAGPAIRS = ARRAY[]::NUMERIC[];
      metric = MREC.value;
      IF(metric ?& array['metric','tags']) THEN
    -- NOTHING
      ELSE
    RAISE NOTICE 'Invalid Metric JSON: %. Missing metric or tags', metric;
      END IF;
      TAGS = metric->'tags';
      SELECT COUNT(*) INTO TAGCOUNT FROM (SELECT * FROM json_object_keys(TAGS)) X;
      IF(TAGCOUNT=0 OR TAGCOUNT > 8) THEN 
    RAISE NOTICE 'Invalid Metric JSON: %. Invalid Tag Count: %', metric, TAGCOUNT; 
      END IF;
      FOR TAGREC IN SELECT J.KEY, J.VALUE 
      FROM json_each_text(TAGS) J
      LOOP
  PERFORM TAGK(TAGREC.KEY);
  PERFORM TAGV(TAGREC.VALUE);
      END LOOP;
      
      FOR TAGREC IN SELECT J.KEY, J.VALUE 
      FROM json_each_text(TAGS) J, TSD_TAGK K
      WHERE J.KEY = K.NAME
      ORDER  BY K.SORT, K.NAME

      LOOP   
  TAGPAIR = tagpair(TAGREC.KEY, TAGREC.VALUE);
  --RAISE NOTICE 'Ordered Tag Pair: %=%', TAGREC.KEY, TAGREC.VALUE;
  TAGPAIRS = TAGPAIRS || TAGPAIR;
      END LOOP;
  FLATMETRIC = array[flatten(metric)];
      --RAISE NOTICE 'METRIC: %', metric->>'metric';
      --RAISE NOTICE 'TAGPAIRS: %', TAGPAIRS;
      METRICID = METRIC(metric->>'metric');
      SELECT T.FQNID INTO FQN_ID 
      FROM TSD_FQN_TAGPAIR P, TSD_TSMETA T
      WHERE P.FQNID = T.FQNID
      AND T.METRIC_ID = METRICID
      AND TAGPAIR_IDS = TAGPAIRS;
      IF NOT FOUND THEN  
    SELECT NEXTVAL('FQN_TP_SEQ') INTO FQN_TAGPAIR_ID;
    SELECT NEXTVAL('FQN_SEQ') INTO FQN_ID;
    INSERT INTO TSD_TSMETA (FQNID, VERSION, METRIC_ID, FQN)
    VALUES ( FQN_ID, 1, METRICID, FLATMETRIC[1]);
    INSERT INTO TSD_FQN_TAGPAIR (FQN_TP_ID, FQNID, TAGPAIR_IDS, TAGS)
    VALUES (FQN_TAGPAIR_ID, FQN_ID, TAGPAIRS, TAGCOUNT);
      END IF;
    
    -- =================================================================================
    --  Value Processing
    -- =================================================================================
      IF(metric ?& array['value']) THEN
    TSSTR = trim(metric->>'timestamp');
    IF(TSSTR IS NULL) THEN
      IF(metric ?& array['timesecs']) THEN
        TSSTR = trim(metric->>'timesecs');
        TSNUM = CAST(TSSTR as NUMERIC);
        TS = to_timestamp(TSNUM);
      ELSE
        TS = now();
      END IF;
    ELSE
      TSNUM = CAST(TSSTR as NUMERIC);
      TS = to_timestamp(TSNUM::double precision / 1000);
    END IF;
    BEGIN
  INSERT INTO TSDB VALUES(TS, FQN_ID, CAST(metric->>'value' as NUMERIC));
  EXCEPTION
    WHEN unique_violation THEN
      perform pg_notify('ERRORS', 'UNIQ VIOL ON TSDB [' || TS || ',' || FQN_ID || ']');
    END;
    END IF;
    raise notice 'metric: id: %, name: %, ex: %', FQN_ID, FLATMETRIC, IDMAP->FLATMETRIC[1];
    if(IDMAP->FLATMETRIC[1] IS NULL) THEN
  IDMAP = jsonb_insert(IDMAP, FLATMETRIC, jsonb('' || FQN_ID));
    END IF;
    END LOOP;
    -- =================================================================================
    raise notice 'idmap: %', IDMAP;
    RETURN IDMAP;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;




CREATE OR REPLACE FUNCTION fqnids(fqnpattern text)
  RETURNS SETOF NUMERIC AS
$func$
select fqnid from tsd_tsmeta where fqnid in (
 select unnest(test_dynamic(fqnpattern))
)
$func$  LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION fqns(fqnpattern text)
  RETURNS SETOF TEXT AS
$func$
select fqn from tsd_tsmeta where fqnid in (
 select unnest(test_dynamic(fqnpattern))
)
$func$  LANGUAGE sql IMMUTABLE;


CREATE OR REPLACE FUNCTION tsd_id(metric text)
  RETURNS numeric AS
$BODY$
BEGIN
    RETURN tsd_id(jsonb(metric));
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;



CREATE OR REPLACE FUNCTION fast_put(times numeric[], ids numeric[], vals numeric[])
  RETURNS int[] AS
$BODY$
DECLARE
  success int = 0;
  failed int = 0;
  tcount int = array_length(times, 1);
  icount int = array_length(ids, 1);
  vcount int = array_length(vals, 1);
BEGIN
    if(tcount != icount OR icount != vcount) THEN
  raise exception 'Uneven array sizes: times: %, ids: %, values: %', tcount, icount, vcount;
    END IF;
    FOR i in 1..tcount LOOP
        BEGIN
  INSERT INTO TSDB (time, fqnid, value)
  VALUES (to_timestamp(times[i]::double precision / 1000), ids[i], vals[i]);
  success = success + 1;
  EXCEPTION WHEN OTHERS THEN
    failed = failed + 1;    
  END;
    END LOOP;
    RETURN array[success, failed];
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;







CREATE OR REPLACE FUNCTION tss()
  RETURNS TEXT AS
$BODY$
DECLARE
  rightnow TIMESTAMP = now();
  ts VARCHAR(20) = '' ||  FLOOR((SELECT EXTRACT(EPOCH FROM rightnow) * 1000 ) + (SELECT EXTRACT(MILLISECONDS FROM rightnow)));
BEGIN
  return ts;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
  
CREATE OR REPLACE FUNCTION tsl()
  RETURNS NUMERIC AS
$BODY$
DECLARE
  rightnow TIMESTAMP = now();
  ts NUMERIC = FLOOR((SELECT EXTRACT(EPOCH FROM rightnow) * 1000 ) + (SELECT EXTRACT(MILLISECONDS FROM rightnow)));
BEGIN
  return ts;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;


CREATE OR REPLACE FUNCTION sort_tagpairids_for_kv(variadic tagpair_ids numeric[])
  RETURNS NUMERIC[] AS
$BODY$
DECLARE
  tagpair_arr NUMERIC[];    
BEGIN
  select array_agg(tagpair_id) from (
    select tagpair_id
    into tagpair_arr
    from tsd_tagpair p, tsd_tagk k
    where p.tagk_id = k.tagk_id
    and p.tagpair_id = ANY(tagpair_ids)
    order by sort, name
 ) as tagpair_id;
  return tagpair_arr;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

CREATE OR REPLACE FUNCTION tagpairs_for_kv(key text, value text)
  RETURNS NUMERIC[] AS
$BODY$
DECLARE
  tagpair_ids NUMERIC[];
  key_pattern TEXT = replace(key, '*', '%');
  value_pattern TEXT = replace(value, '*', '%');
BEGIN
  select array_agg(distinct tagpair_id) 
  into tagpair_ids 
  from tsd_tagpair t, tsd_tagk k, tsd_tagv v 
  where t.tagk_id = k.tagk_id 
  and t.tagv_id = v.tagv_id 
  and k.name like key_pattern 
  and v.name like value_pattern;
  return tagpair_ids;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

  
CREATE OR REPLACE FUNCTION sort_tagpairids_for_kv(tagpair_ids numeric[])
  RETURNS NUMERIC[] AS
$BODY$
DECLARE
  tagpair_arr NUMERIC[];    
BEGIN
  select array_agg(tagpair_id) from (
    select tagpair_id
    into tagpair_arr
    from tsd_tagpair p, tsd_tagk k
    where p.tagk_id = k.tagk_id
    and p.tagpair_id = ANY(tagpair_ids)
    order by sort, name
 ) as tagpair_id;
  return tagpair_arr;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;



--  constant_name CONSTANT data_type := expression;




SELECT TAGK('dc', 0);
SELECT TAGK('host', 1);
SELECT TAGK('app', 2);
SELECT TAGK('cpu');
SELECT TAGK('type');

SELECT TAGV('0');
SELECT TAGV('1');
SELECT TAGV('2');
SELECT TAGV('3');
SELECT TAGV('4');
SELECT TAGV('5');
SELECT TAGV('6');
SELECT TAGV('7');
SELECT TAGV('combined');
SELECT TAGV('idle');
SELECT TAGV('irq');
SELECT TAGV('nice');
SELECT TAGV('softirq');
SELECT TAGV('stolen');
SELECT TAGV('sys');
SELECT TAGV('user');
SELECT TAGV('wait');
SELECT TAGV('heliosleopard');

SELECT METRIC('sys.cpu');
SELECT METRIC('sys.fs.avail');

SELECT TAGPAIR('host', 'heliosleopard');
select tagpair('cpu', '0');
select tagpair('cpu', '1');
select tagpair('cpu', '2');
select tagpair('cpu', '3');
select tagpair('cpu', '4');
select tagpair('cpu', '5');
select tagpair('cpu', '6');
select tagpair('cpu', '7');

select tagpair('type', 'combined');
select tagpair('type', 'idle');
select tagpair('type', 'irq');
select tagpair('type', 'nice');
select tagpair('type', 'softirq');
select tagpair('type', 'stolen');
select tagpair('type', 'sys');
select tagpair('type', 'user');
select tagpair('type', 'wait');



select tsd_id(jsonb('[' ||
    '{"metric" : "sys.cpu", "tags" : {"dc" : "dal3", "host" : "web01", "app" : "edge-web"}, "timestamp" :' || tss() || ', "value" : 384734},' ||
    '{"metric" : "sys.cpu", "tags" : {"dc" : "dal4", "host" : "web01", "app" : "edge-web"}, "timestamp" :' || tss() || ', "value" : 384734},' ||
    '{"metric" : "sys.cpu", "tags" : {"dc" : "dal5", "host" : "web01", "app" : "edge-web"}, "timestamp" :' || tss() || ', "value" : 384734},' ||
    '{"metric" : "sys.cpu", "tags" : {"dc" : "dal3", "host" : "web02", "app" : "edge-web"}, "timestamp" :' || tss() || ', "value" : 384734},' ||
    '{"metric" : "sys.cpu", "tags" : {"dc" : "dal4", "host" : "web02", "app" : "edge-web"}, "timestamp" :' || tss() || ', "value" : 384734},' ||
    '{"metric" : "sys.cpu", "tags" : {"dc" : "dal5", "host" : "web02", "app" : "edge-web"}, "timestamp" :' || tss() || ', "value" : 384734},' ||
    '{"metric" : "sys.cpu", "tags" : {"dc" : "dal3", "host" : "web03", "app" : "edge-web"}, "timestamp" :' || tss() || ', "value" : 384734},' ||
    '{"metric" : "sys.cpu", "tags" : {"dc" : "dal4", "host" : "web03", "app" : "edge-web"}, "timestamp" :' || tss() || ', "value" : 384734},' ||
    '{"metric" : "sys.cpu", "tags" : {"dc" : "dal5", "host" : "web03", "app" : "edge-web"}, "timestamp" :' || tss() || ', "value" : 384734},' ||
    '{"metric" : "sys.xyz", "tags" : {"dc" : "dal3", "host" : "web01", "app" : "edge-web"}, "timestamp" :' || tss() || ', "value" : 384734},' ||
    '{"metric" : "sys.xyz", "tags" : {"dc" : "dal4", "host" : "web01", "app" : "edge-web"}, "timestamp" :' || tss() || ', "value" : 384734},' ||
    '{"metric" : "sys.xyz", "tags" : {"dc" : "dal5", "host" : "web01", "app" : "edge-web"}, "timestamp" :' || tss() || ', "value" : 384734},' ||
    '{"metric" : "sys.xyz", "tags" : {"dc" : "dal3", "host" : "web02", "app" : "edge-web"}, "timestamp" :' || tss() || ', "value" : 384734},' ||
    '{"metric" : "sys.xyz", "tags" : {"dc" : "dal4", "host" : "web02", "app" : "edge-web"}, "timestamp" :' || tss() || ', "value" : 384734},' ||
    '{"metric" : "sys.xyz", "tags" : {"dc" : "dal5", "host" : "web02", "app" : "edge-web"}, "timestamp" :' || tss() || ', "value" : 384734},' ||
    '{"metric" : "sys.xyz", "tags" : {"dc" : "dal3", "host" : "web03", "app" : "edge-web"}, "timestamp" :' || tss() || ', "value" : 384734},' ||
    '{"metric" : "sys.xyz", "tags" : {"dc" : "dal4", "host" : "web03", "app" : "edge-web"}, "timestamp" :' || tss() || ', "value" : 384734},' ||
    '{"metric" : "sys.xyz", "tags" : {"dc" : "dal5", "host" : "web03", "app" : "edge-web", "foo" : "bar"}, "timestamp" :' || tss() || ', "value" : 384734}' ||  
      
  ']'))


-- GROOVY:
v = """
select tsd_id(jsonb('[' ||
    '{"metric" : "sys.cpu", "tags" : {"dc" : "dal3", "host" : "web01", "app" : "edge-web"}, "timestamp" : $ts, "value" : 384734},' ||
    '{"metric" : "sys.cpu", "tags" : {"dc" : "dal4", "host" : "web01", "app" : "edge-web"}, "timestamp" : $ts, "value" : 384734},' ||
    '{"metric" : "sys.cpu", "tags" : {"dc" : "dal5", "host" : "web01", "app" : "edge-web"}, "timestamp" : $ts, "value" : 384734},' ||
    '{"metric" : "sys.cpu", "tags" : {"dc" : "dal3", "host" : "web02", "app" : "edge-web"}, "timestamp" : $ts, "value" : 384734},' ||
    '{"metric" : "sys.cpu", "tags" : {"dc" : "dal4", "host" : "web02", "app" : "edge-web"}, "timestamp" : $ts, "value" : 384734},' ||
    '{"metric" : "sys.cpu", "tags" : {"dc" : "dal5", "host" : "web02", "app" : "edge-web"}, "timestamp" : $ts, "value" : 384734},' ||
    '{"metric" : "sys.cpu", "tags" : {"dc" : "dal3", "host" : "web03", "app" : "edge-web"}, "timestamp" : $ts, "value" : 384734},' ||
    '{"metric" : "sys.cpu", "tags" : {"dc" : "dal4", "host" : "web03", "app" : "edge-web"}, "timestamp" : $ts, "value" : 384734},' ||
    '{"metric" : "sys.cpu", "tags" : {"dc" : "dal5", "host" : "web03", "app" : "edge-web"}, "timestamp" : $ts, "value" : 384734}' ||  
  ']'))
"""



select array_agg(tagpairid(X)) from (select unnest(tagpair_ids) as X from tsd_fqn_tagpair where fqn_tp_id = 4724) as id

select array_to_string(tagpairid(X), '=') from (select unnest(tagpair_ids) as X from tsd_fqn_tagpair where fqn_tp_id = 4724) as id

select array_agg(array_to_string(tagpairid(X), '=')) from (select unnest(tagpair_ids) as X from tsd_fqn_tagpair where fqn_tp_id = 4724) as id



select array["1", "2", "3", "4", "5"] from (
  select 
  unnest(tagpairs_for_kv('dc', 'dcx')) as "1",
  unnest(tagpairs_for_kv('host', 'mad-server')) as "2",
  unnest(tagpairs_for_kv('app', 'os-agent')) as "3",
  unnest(tagpairs_for_kv('cpu', '0')) as "4",
  unnest(tagpairs_for_kv('type', '*')) as "5"
) as rx

select tsd_id(jsonb('[' ||
    '{"metric" : "sys.cpu", "tags" : {"dc" : "dal3", "host" : "web01", "app" : "edge-web"}, "timestamp" : $ts, "value" : 384734},' ||
    '{"metric" : "sys.cpu", "tags" : {"dc" : "dal4", "host" : "web01", "app" : "edge-web"}, "timestamp" : $ts, "value" : 384734},' ||
    '{"metric" : "sys.cpu", "tags" : {"dc" : "dal5", "host" : "web01", "app" : "edge-web"}, "timestamp" : $ts, "value" : 384734},' ||
    '{"metric" : "sys.cpu", "tags" : {"dc" : "dal3", "host" : "web02", "app" : "edge-web"}, "timestamp" : $ts, "value" : 384734},' ||
    '{"metric" : "sys.cpu", "tags" : {"dc" : "dal4", "host" : "web02", "app" : "edge-web"}, "timestamp" : $ts, "value" : 384734},' ||
    '{"metric" : "sys.cpu", "tags" : {"dc" : "dal5", "host" : "web02", "app" : "edge-web"}, "timestamp" : $ts, "value" : 384734},' ||
    '{"metric" : "sys.cpu", "tags" : {"dc" : "dal3", "host" : "web03", "app" : "edge-web"}, "timestamp" : $ts, "value" : 384734},' ||
    '{"metric" : "sys.cpu", "tags" : {"dc" : "dal4", "host" : "web03", "app" : "edge-web"}, "timestamp" : $ts, "value" : 384734},' ||
    '{"metric" : "sys.cpu", "tags" : {"dc" : "dal5", "host" : "web03", "app" : "edge-web"}, "timestamp" : $ts, "value" : 384734}' ||  
  ']'))


select tsd_id_for_ids(jsonb('[' ||
    '{"metric" : "sys.cpu", "tags" : {"dc" : "dal3", "host" : "web01", "app" : "edge-web"}},' ||
    '{"metric" : "sys.cpu", "tags" : {"dc" : "dal4", "host" : "web01", "app" : "edge-web"}},' ||
    '{"metric" : "sys.cpu", "tags" : {"dc" : "dal5", "host" : "web01", "app" : "edge-web"}},' ||
    '{"metric" : "sys.cpu", "tags" : {"dc" : "dal3", "host" : "web02", "app" : "edge-web"}},' ||
    '{"metric" : "sys.cpu", "tags" : {"dc" : "dal4", "host" : "web02", "app" : "edge-web"}},' ||
    '{"metric" : "sys.cpu", "tags" : {"dc" : "dal5", "host" : "web02", "app" : "edge-web"}},' ||
    '{"metric" : "sys.cpu", "tags" : {"dc" : "dal3", "host" : "web03", "app" : "edge-web"}},' ||
    '{"metric" : "sys.cpu", "tags" : {"dc" : "dal4", "host" : "web03", "app" : "edge-web"}},' ||
    '{"metric" : "sys.cpu", "tags" : {"dc" : "dal5", "host" : "web03", "app" : "edge-web"}}' ||  
  ']'))