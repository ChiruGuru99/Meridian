-- One Postgres instance, one database per service, separate owner (migrations) and app (runtime) roles.
-- Simulated environment: passwords are local-only and equal to the role name.

CREATE ROLE ledger_owner LOGIN PASSWORD 'ledger_owner';
CREATE ROLE ledger_app LOGIN PASSWORD 'ledger_app';
CREATE DATABASE ledger OWNER ledger_owner;

CREATE ROLE gateway LOGIN PASSWORD 'gateway';
CREATE DATABASE gateway OWNER gateway;

CREATE ROLE recon LOGIN PASSWORD 'recon';
CREATE DATABASE recon OWNER recon;

\connect ledger
ALTER SCHEMA public OWNER TO ledger_owner;
