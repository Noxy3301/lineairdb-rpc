# LineairDB Remote Storage Engine for MySQL

A MySQL storage engine that remotely communicates with LineairDB. Separating LineairDB into a standalone process enables multiple MySQL servers to share the same high-performance transaction engine.

## Quick Start

```bash
git clone --recursive https://github.com/Noxy3301/lineairdb-rpc.git
cd lineairdb-rpc
./build.sh
```

### Initialize and Start MySQL Server

```bash
cd build
./runtime_output_directory/mysqld --initialize-insecure --user=$USER --datadir=./data
./runtime_output_directory/mysqld --datadir=./data --socket=/tmp/mysql.sock --port=3307 &
```

### Running LineairDB Service

```bash
./build/lineairdb_service/lineairdb-service 
```

### Connect to MySQL and Install LineairDB Plugin

```bash
./build/runtime_output_directory/mysql -u root --socket=/tmp/mysql.sock --port=3307
```

Then run the following SQL command:
```sql
INSTALL PLUGIN lineairdb SONAME 'ha_lineairdb_storage_engine.so';
```

#### Simple Example: Using MySQL with LineairDB-RPC

```bash
SHOW ENGINES;

DROP DATABASE IF EXISTS lineairdb_test;
CREATE DATABASE lineairdb_test;
USE lineairdb_test;

CREATE TABLE test (
    id INT PRIMARY KEY,
    name VARCHAR(20)
) ENGINE=LINEAIRDB;

INSERT INTO test VALUES (1, 'hello');
INSERT INTO test VALUES (2, 'world');

SELECT * FROM test;

SELECT * FROM test WHERE id = 1;
```

## Important Notes

> [!WARNING]
> `/bench` and `/test` directories have not been verified to work with the current RPC implementation.
