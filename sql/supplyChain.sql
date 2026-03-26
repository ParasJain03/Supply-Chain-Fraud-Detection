
USE ROLE ACCOUNTADMIN;

-- Create database, schema, warehouse
CREATE DATABASE IF NOT EXISTS SUPPLY_CHAIN_DB;
USE DATABASE SUPPLY_CHAIN_DB;
USE SCHEMA PUBLIC;

CREATE WAREHOUSE SUPPLY_WH
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;
USE WAREHOUSE SUPPLY_WH;

-- Original tables (with all columns)
CREATE TABLE SELLERS (
  nodeId       NUMBER PRIMARY KEY,
  seller_name  STRING,
  platform     STRING,
  city         STRING,
  gst_number   STRING,
  bank_account STRING,
  return_rate  FLOAT,
  reg_days_ago NUMBER,
  fraud_flag   NUMBER
);

CREATE TABLE WAREHOUSES (
  nodeId         NUMBER PRIMARY KEY,
  warehouse_name STRING,
  city           STRING,
  pin_code       STRING
);

CREATE TABLE ORDERS (
  sourceNodeId    NUMBER,
  targetNodeId    NUMBER,
  order_id        STRING,
  order_value     FLOAT,
  delivery_delay  NUMBER,
  return_claimed  NUMBER,
  route_deviation NUMBER
);

CREATE TABLE LOGISTICS (
  sourceNodeId NUMBER,
  targetNodeId NUMBER,
  courier      STRING,
  transit_days NUMBER,
  loss_count   NUMBER
);

-- Neo4j roles and permissions
CREATE ROLE IF NOT EXISTS gds_role;
GRANT APPLICATION ROLE NEO4J_GRAPH_ANALYTICS.app_user TO ROLE gds_role;
GRANT ROLE gds_role TO USER PARASJAIN;
GRANT USAGE ON WAREHOUSE SUPPLY_WH TO ROLE gds_role;

CREATE DATABASE ROLE SUPPLY_CHAIN_DB.gds_db_role;
GRANT DATABASE ROLE SUPPLY_CHAIN_DB.gds_db_role
    TO APPLICATION NEO4J_GRAPH_ANALYTICS;

GRANT USAGE ON DATABASE SUPPLY_CHAIN_DB
    TO DATABASE ROLE SUPPLY_CHAIN_DB.gds_db_role;
GRANT USAGE ON SCHEMA SUPPLY_CHAIN_DB.PUBLIC
    TO DATABASE ROLE SUPPLY_CHAIN_DB.gds_db_role;
GRANT CREATE TABLE ON SCHEMA SUPPLY_CHAIN_DB.PUBLIC
    TO DATABASE ROLE SUPPLY_CHAIN_DB.gds_db_role;
GRANT SELECT, INSERT ON FUTURE TABLES IN SCHEMA SUPPLY_CHAIN_DB.PUBLIC
    TO DATABASE ROLE SUPPLY_CHAIN_DB.gds_db_role;

SELECT 'Snowflake setup complete!' AS status;


USE ROLE ACCOUNTADMIN;
USE DATABASE SUPPLY_CHAIN_DB;
USE SCHEMA PUBLIC;
USE WAREHOUSE SUPPLY_WH;

USE ROLE ACCOUNTADMIN;
USE DATABASE SUPPLY_CHAIN_DB;
USE SCHEMA PUBLIC;
USE WAREHOUSE SUPPLY_WH;

CALL NEO4J_GRAPH_ANALYTICS.graph.louvain('CPU_X64_XS', {
    'project': {
        'nodeTables': [
            'SUPPLY_CHAIN_DB.PUBLIC.SELLERS_GRAPH',
            'SUPPLY_CHAIN_DB.PUBLIC.WAREHOUSES_GRAPH'
        ],
        'relationshipTables': {
            'SUPPLY_CHAIN_DB.PUBLIC.ORDERS_GRAPH': {
                'sourceTable': 'SUPPLY_CHAIN_DB.PUBLIC.SELLERS_GRAPH',
                'targetTable': 'SUPPLY_CHAIN_DB.PUBLIC.WAREHOUSES_GRAPH',
                'orientation': 'NATURAL'
            }
        }
    },
    'compute': { 'consecutiveIds': true },
    'write': [{
        'nodeLabel': 'SELLERS_GRAPH',
        'outputTable': 'SUPPLY_CHAIN_DB.PUBLIC.SELLER_COMMUNITIES'
    }]
});
CALL NEO4J_GRAPH_ANALYTICS.graph.wcc('CPU_X64_XS', {
    'project': {
        'nodeTables': [
            'SUPPLY_CHAIN_DB.PUBLIC.SELLERS_GRAPH'
        ],
        'relationshipTables': {
            'SUPPLY_CHAIN_DB.PUBLIC.SHARED_BANK_EDGES_GRAPH': {
                'sourceTable': 'SUPPLY_CHAIN_DB.PUBLIC.SELLERS_GRAPH',
                'targetTable': 'SUPPLY_CHAIN_DB.PUBLIC.SELLERS_GRAPH',
                'orientation': 'UNDIRECTED'
            }
        }
    },
    'compute': {},
    'write': [{
        'nodeLabel': 'SELLERS_GRAPH',
        'outputTable': 'SUPPLY_CHAIN_DB.PUBLIC.SELLER_WCC'
    }]
});
CALL NEO4J_GRAPH_ANALYTICS.graph.page_rank('CPU_X64_XS', {
    'project': {
        'nodeTables': [
            'SUPPLY_CHAIN_DB.PUBLIC.SELLERS_GRAPH',
            'SUPPLY_CHAIN_DB.PUBLIC.WAREHOUSES_GRAPH'
        ],
        'relationshipTables': {
            'SUPPLY_CHAIN_DB.PUBLIC.ORDERS_GRAPH': {
                'sourceTable': 'SUPPLY_CHAIN_DB.PUBLIC.SELLERS_GRAPH',
                'targetTable': 'SUPPLY_CHAIN_DB.PUBLIC.WAREHOUSES_GRAPH',
                'orientation': 'NATURAL'
            }
        }
    },
    'compute': { 'dampingFactor': 0.85, 'maxIterations': 20 },
    'write': [{
        'nodeLabel': 'SELLERS_GRAPH',
        'outputTable': 'SUPPLY_CHAIN_DB.PUBLIC.SELLER_PAGERANK'
    }]
});


CALL NEO4J_GRAPH_ANALYTICS.graph.page_rank('CPU_X64_XS', {
    'project': {
        'nodeTables': [
            'SUPPLY_CHAIN_DB.PUBLIC.SELLERS_GRAPH',
            'SUPPLY_CHAIN_DB.PUBLIC.WAREHOUSES_GRAPH'
        ],
        'relationshipTables': {
            'SUPPLY_CHAIN_DB.PUBLIC.ORDERS_GRAPH': {
                'sourceTable': 'SUPPLY_CHAIN_DB.PUBLIC.SELLERS_GRAPH',
                'targetTable': 'SUPPLY_CHAIN_DB.PUBLIC.WAREHOUSES_GRAPH',
                'orientation': 'NATURAL'
            }
        }
    },
    'compute': { 'dampingFactor': 0.85, 'maxIterations': 20 },
    'write': [{
        'nodeLabel': 'SELLERS_GRAPH',
        'outputTable': 'SUPPLY_CHAIN_DB.PUBLIC.SELLER_PAGERANK'
    }]
});

CREATE OR REPLACE TABLE SELLER_RISK_MASTER AS
SELECT
    s.nodeId,
    s.seller_name,
    s.platform,
    s.city,
    s.return_rate,
    s.fraud_flag,
    s.bank_account,
    p.pagerank                                   AS pagerank_score,
    c.community                                  AS louvain_community,
    w.component                                  AS wcc_entity,
    ROUND(
        (s.return_rate * 30) +
        (p.pagerank    * 20) +
        (s.fraud_flag  * 50), 2
    )                                            AS risk_score,
    CASE
        WHEN (s.return_rate*30 + p.pagerank*20 + s.fraud_flag*50) > 60 THEN 'HIGH'
        WHEN (s.return_rate*30 + p.pagerank*20 + s.fraud_flag*50) > 30 THEN 'MEDIUM'
        ELSE 'LOW'
    END                                          AS risk_level
FROM SELLERS s
LEFT JOIN SELLER_PAGERANK    p ON s.nodeId = p.nodeId
LEFT JOIN SELLER_COMMUNITIES c ON s.nodeId = c.nodeId
LEFT JOIN SELLER_WCC         w ON s.nodeId = w.nodeId
ORDER BY risk_score DESC;

SELECT * FROM SELLER_RISK_MASTER LIMIT 5;

-- ============================================================
-- PART 2: REAL-TIME FRAUD DETECTION PIPELINE
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE SUPPLY_CHAIN_DB;
USE SCHEMA PUBLIC;
USE WAREHOUSE SUPPLY_WH;

-- ============================================================
-- STEP 1: STREAMS — Track changes on source tables
-- ============================================================

CREATE OR REPLACE STREAM ORDERS_STREAM ON TABLE SUPPLY_CHAIN_DB.PUBLIC.ORDERS
  APPEND_ONLY = TRUE;

CREATE OR REPLACE STREAM SELLERS_STREAM ON TABLE SUPPLY_CHAIN_DB.PUBLIC.SELLERS
  APPEND_ONLY = TRUE;

-- ============================================================
-- STEP 2: DYNAMIC TABLE — Auto-refreshing risk scoring
-- ============================================================

CREATE OR REPLACE DYNAMIC TABLE SELLER_RISK_REALTIME
  TARGET_LAG = '1 minute'
  WAREHOUSE = SUPPLY_WH
AS
SELECT
    s.nodeId,
    s.seller_name,
    s.platform,
    s.city,
    s.return_rate,
    s.fraud_flag,
    s.bank_account,
    p.pagerank                                   AS pagerank_score,
    c.community                                  AS louvain_community,
    w.component                                  AS wcc_entity,
    ROUND(
        (s.return_rate * 30) +
        (p.pagerank    * 20) +
        (s.fraud_flag  * 50), 2
    )                                            AS risk_score,
    CASE
        WHEN (s.return_rate*30 + p.pagerank*20 + s.fraud_flag*50) > 60 THEN 'HIGH'
        WHEN (s.return_rate*30 + p.pagerank*20 + s.fraud_flag*50) > 30 THEN 'MEDIUM'
        ELSE 'LOW'
    END                                          AS risk_level,
    CURRENT_TIMESTAMP()                          AS last_refreshed
FROM SUPPLY_CHAIN_DB.PUBLIC.SELLERS s
LEFT JOIN SUPPLY_CHAIN_DB.PUBLIC.SELLER_PAGERANK    p ON s.nodeId = p.nodeId
LEFT JOIN SUPPLY_CHAIN_DB.PUBLIC.SELLER_COMMUNITIES c ON s.nodeId = c.nodeId
LEFT JOIN SUPPLY_CHAIN_DB.PUBLIC.SELLER_WCC         w ON s.nodeId = w.nodeId;

-- ============================================================
-- STEP 3: SCHEDULED TASK GRAPH — Re-run graph algorithms
-- ============================================================

CREATE OR REPLACE TASK GRAPH_ANALYTICS_ROOT
  WAREHOUSE = SUPPLY_WH
  SCHEDULE = '15 MINUTE'
  SUSPEND_TASK_AFTER_NUM_FAILURES = 3
  TASK_AUTO_RETRY_ATTEMPTS = 1
AS
  SELECT 'Starting graph analytics refresh' AS status;

CREATE OR REPLACE TASK GRAPH_LOUVAIN
  WAREHOUSE = SUPPLY_WH
  AFTER GRAPH_ANALYTICS_ROOT
AS
  CALL NEO4J_GRAPH_ANALYTICS.graph.louvain('CPU_X64_XS', {
      'project': {
          'nodeTables': [
              'SUPPLY_CHAIN_DB.PUBLIC.SELLERS_GRAPH',
              'SUPPLY_CHAIN_DB.PUBLIC.WAREHOUSES_GRAPH'
          ],
          'relationshipTables': {
              'SUPPLY_CHAIN_DB.PUBLIC.ORDERS_GRAPH': {
                  'sourceTable': 'SUPPLY_CHAIN_DB.PUBLIC.SELLERS_GRAPH',
                  'targetTable': 'SUPPLY_CHAIN_DB.PUBLIC.WAREHOUSES_GRAPH',
                  'orientation': 'NATURAL'
              }
          }
      },
      'compute': { 'consecutiveIds': true },
      'write': [{
          'nodeLabel': 'SELLERS_GRAPH',
          'outputTable': 'SUPPLY_CHAIN_DB.PUBLIC.SELLER_COMMUNITIES'
      }]
  });

CREATE OR REPLACE TASK GRAPH_WCC
  WAREHOUSE = SUPPLY_WH
  AFTER GRAPH_ANALYTICS_ROOT
AS
  CALL NEO4J_GRAPH_ANALYTICS.graph.wcc('CPU_X64_XS', {
      'project': {
          'nodeTables': [
              'SUPPLY_CHAIN_DB.PUBLIC.SELLERS_GRAPH'
          ],
          'relationshipTables': {
              'SUPPLY_CHAIN_DB.PUBLIC.SHARED_BANK_EDGES_GRAPH': {
                  'sourceTable': 'SUPPLY_CHAIN_DB.PUBLIC.SELLERS_GRAPH',
                  'targetTable': 'SUPPLY_CHAIN_DB.PUBLIC.SELLERS_GRAPH',
                  'orientation': 'UNDIRECTED'
              }
          }
      },
      'compute': {},
      'write': [{
          'nodeLabel': 'SELLERS_GRAPH',
          'outputTable': 'SUPPLY_CHAIN_DB.PUBLIC.SELLER_WCC'
      }]
  });

CREATE OR REPLACE TASK GRAPH_PAGERANK
  WAREHOUSE = SUPPLY_WH
  AFTER GRAPH_ANALYTICS_ROOT
AS
  CALL NEO4J_GRAPH_ANALYTICS.graph.page_rank('CPU_X64_XS', {
      'project': {
          'nodeTables': [
              'SUPPLY_CHAIN_DB.PUBLIC.SELLERS_GRAPH',
              'SUPPLY_CHAIN_DB.PUBLIC.WAREHOUSES_GRAPH'
          ],
          'relationshipTables': {
              'SUPPLY_CHAIN_DB.PUBLIC.ORDERS_GRAPH': {
                  'sourceTable': 'SUPPLY_CHAIN_DB.PUBLIC.SELLERS_GRAPH',
                  'targetTable': 'SUPPLY_CHAIN_DB.PUBLIC.WAREHOUSES_GRAPH',
                  'orientation': 'NATURAL'
              }
          }
      },
      'compute': { 'dampingFactor': 0.85, 'maxIterations': 20 },
      'write': [{
          'nodeLabel': 'SELLERS_GRAPH',
          'outputTable': 'SUPPLY_CHAIN_DB.PUBLIC.SELLER_PAGERANK'
      }]
  });

-- ============================================================
-- STEP 4: ALERT — Notify on new HIGH-risk sellers
-- ============================================================

CREATE OR REPLACE TABLE SUPPLY_CHAIN_DB.PUBLIC.FRAUD_ALERT_LOG (
    alert_time      TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    seller_nodeid   NUMBER,
    seller_name     STRING,
    risk_score      FLOAT,
    risk_level      STRING,
    city            STRING,
    platform        STRING,
    wcc_entity      NUMBER,
    louvain_community NUMBER,
    alert_type      STRING
);

CREATE NOTIFICATION INTEGRATION IF NOT EXISTS FRAUD_EMAIL_INT
  TYPE = EMAIL
  ENABLED = TRUE;

CREATE OR REPLACE ALERT HIGH_RISK_SELLER_ALERT
  WAREHOUSE = SUPPLY_WH
  SCHEDULE = '5 MINUTE'
IF (EXISTS (
    SELECT 1 FROM SUPPLY_CHAIN_DB.PUBLIC.SELLER_RISK_REALTIME
    WHERE risk_level = 'HIGH'
      AND nodeId NOT IN (SELECT seller_nodeid FROM SUPPLY_CHAIN_DB.PUBLIC.FRAUD_ALERT_LOG)
))
THEN
  INSERT INTO SUPPLY_CHAIN_DB.PUBLIC.FRAUD_ALERT_LOG
      (seller_nodeid, seller_name, risk_score, risk_level, city, platform, wcc_entity, louvain_community, alert_type)
  SELECT nodeId, seller_name, risk_score, risk_level, city, platform, wcc_entity, louvain_community, 'NEW_HIGH_RISK'
  FROM SUPPLY_CHAIN_DB.PUBLIC.SELLER_RISK_REALTIME
  WHERE risk_level = 'HIGH'
    AND nodeId NOT IN (SELECT seller_nodeid FROM SUPPLY_CHAIN_DB.PUBLIC.FRAUD_ALERT_LOG);

CREATE OR REPLACE TASK FRAUD_EMAIL_NOTIFIER
  WAREHOUSE = SUPPLY_WH
  SCHEDULE = '10 MINUTE'
AS
  CALL SYSTEM$SEND_EMAIL(
      'FRAUD_EMAIL_INT',
      'PARASJAIN',
      'FRAUD ALERT: High-Risk Sellers Summary',
      (SELECT 'High-risk sellers in system: ' || COUNT(*) || '. Check FRAUD_ALERT_LOG for details.'
       FROM SUPPLY_CHAIN_DB.PUBLIC.FRAUD_ALERT_LOG
       WHERE alert_time >= DATEADD('minute', -10, CURRENT_TIMESTAMP()))
  );

-- ============================================================
-- STEP 5: RESUME TASKS & ALERT
-- ============================================================

ALTER TASK GRAPH_PAGERANK RESUME;
ALTER TASK GRAPH_WCC RESUME;
ALTER TASK GRAPH_LOUVAIN RESUME;
ALTER TASK GRAPH_ANALYTICS_ROOT RESUME;

ALTER ALERT HIGH_RISK_SELLER_ALERT RESUME;
ALTER TASK FRAUD_EMAIL_NOTIFIER RESUME;

-- Verify pipeline status
SHOW TASKS IN SCHEMA SUPPLY_CHAIN_DB.PUBLIC;
SHOW ALERTS IN SCHEMA SUPPLY_CHAIN_DB.PUBLIC;
SHOW DYNAMIC TABLES IN SCHEMA SUPPLY_CHAIN_DB.PUBLIC;

-- ============================================================
-- STEP 6: STREAMLIT DASHBOARD
-- Deploy via Snowsight UI: Projects → Streamlit → + Streamlit App
-- Upload /streamlit_app.py, select SUPPLY_WH and SUPPLY_CHAIN_DB
-- ============================================================
