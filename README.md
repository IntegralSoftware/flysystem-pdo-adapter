Flysystem PDO Database Adapter
==============================

[![Build Status](https://img.shields.io/travis/IntegralSoftware/flysystem-pdo-adapter/master.svg?style=flat-square)](https://travis-ci.org/IntegralSoftware/flysystem-pdo-adapter)
[![License](https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square)](https://opensource.org/licenses/MIT)

PDO database adapter for [Flysystem](https://github.com/thephpleague/flysystem) filesystem abstraction. No additional dependencies, only PDO extension required.

## Installation

```
composer require integral/flysystem-pdo-adapter
```

## Database Configuration

At the beginning you have to create a table that will be used to store files.

SQL table schema examples for MySQL and SQLite are presented below.

**MySQL**
```sql
CREATE TABLE files (
  id int(11) NOT NULL AUTO_INCREMENT,
  path varchar(255) NOT NULL,
  type enum('file','dir') NOT NULL,
  contents longblob,
  size int(11) NOT NULL DEFAULT 0,
  mimetype varchar(127),
  timestamp int(11) NOT NULL DEFAULT 0,
  PRIMARY KEY (id),
  UNIQUE KEY path_unique (path)
);
```

**SQLite**
```sql
CREATE TABLE files (
    id INTEGER PRIMARY KEY,
    path TEXT NOT NULL UNIQUE,
    type TEXT NOT NULL,
    contents BLOB,
    size INTEGER NOT NULL DEFAULT 0,
    mimetype TEXT,
    timestamp INTEGER NOT NULL DEFAULT 0
)
```


## Usage

Create an adapter by passing a valid `PDO` object and table name as constructor arguments:

**MySQL**
```php
// http://php.net/manual/pl/ref.pdo-mysql.connection.php
$pdo = new PDO('mysql:host=hostname;dbname=database_name', 'username', 'password');
$adapter = new PDOAdapter($pdo, 'files');
```

**SQLite**
```php
// http://php.net/manual/pl/ref.pdo-sqlite.connection.php
$pdo = new PDO('sqlite:/absolute/path/to/database.sqlite');
$adapter = new PDOAdapter($pdo, 'files');
```

Then simply pass the created adapter to `\League\Flysystem\Filesystem`:

```php
$filesystem = new Filesystem($adapter);
```

Done! At this point the `$filesystem` is ready to use.

## Note

This implementation emulates a tree structured filesystem, therefore some of the operations
(like renaming or deleting a folder) produce quite a lot of database queries, which may result
in a poor performance for some scenarios.