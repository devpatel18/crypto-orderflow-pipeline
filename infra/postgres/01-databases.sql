-- Separate database for the Iceberg JDBC catalog (used by Trino and Flink).
-- The default "market" database holds predictions, labels, and run history.
CREATE DATABASE iceberg;
GRANT ALL PRIVILEGES ON DATABASE iceberg TO market;

CREATE DATABASE mlflow;
GRANT ALL PRIVILEGES ON DATABASE mlflow TO market;

-- Trino's Iceberg JDBC catalog does NOT auto-create these tables (it assumes
-- they exist and fails with "Cannot check and eventually update SQL schema").
-- Schema matches Iceberg JdbcCatalog V1 (JdbcUtil).
\connect iceberg

CREATE TABLE iceberg_tables (
    catalog_name VARCHAR(255) NOT NULL,
    table_namespace VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    metadata_location VARCHAR(1000),
    previous_metadata_location VARCHAR(1000),
    iceberg_type VARCHAR(5),
    PRIMARY KEY (catalog_name, table_namespace, table_name)
);

CREATE TABLE iceberg_namespace_properties (
    catalog_name VARCHAR(255) NOT NULL,
    namespace VARCHAR(255) NOT NULL,
    property_key VARCHAR(255) NOT NULL,
    property_value VARCHAR(1000),
    PRIMARY KEY (catalog_name, namespace, property_key)
);

ALTER TABLE iceberg_tables OWNER TO market;
ALTER TABLE iceberg_namespace_properties OWNER TO market;
