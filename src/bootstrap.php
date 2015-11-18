<?php

define('MASTER_MODE', 0);
define('SLAVE_MODE', 1);
define('DEF_DATABASE', 'percona');
define('DEF_TABLE', 'checksums');
define('APP_NAME','myReplicationChecker.phar');
require_once __DIR__.'/../vendor/autoload.php';