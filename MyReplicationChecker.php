<?php
/**
 * MyReplicationChecker, an PHP script to check MySQL replication data consistency
 * 
 * Copyright (C) 2014 Javier Munoz
 * 
 * This file is part of MyReplicationChecker.
 * 
 * MyReplicationChecker is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * MyReplicationChecker is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * 
 * See the GNU General Public License for more details. You should have received a copy of the GNU
 * General Public License along with MyReplicationChecker. If not, see <http://www.gnu.org/licenses/>.
 * 
 * Authors: Javier Munoz <javier@greicodex.com>
 * 
 * The work on this script was done to verify the data on several replicas on our production system.
 * After looking around on the internet I found the amazing tools from Percona. Altough they are fast
 * and complete, they locked my tables to perform the hashing. Since many of my tables are really large
 * (and I mean REALLLLY like +500 million records BIG!!), I decided to write my own tool inspired
 * in part on their design with my own improvements and optmized queries.
 * 
 * Many on my tables have auto-increment primary keys and are normalized so I wrote specials methods
 * to hash those quickly.
 * 
 * Every class has been included on the same file for convenience (I don't like tools with many files
 * lying around polluting my server ;) ).
 * Look at the end for Usage options
 * Features:
 *    - Index the all replicated databases into maneable chunks
 *    - Hash each chunk into a HEX number using MySQL internal CRC32 function
 *    - Report differences between the master and replicas
 *    - Run on Incremental mode (hash chunks randomly)
 *    - Hash each chunk in parallel on each DB to avoid differences due to normal production activity.
 *    - Store the results on each DB to later reference and analysis or posible re-synching
 *    - Hash chunks of up to 1 million records in under 7 seconds on a Quad-Core 8GB system
 *    - Separate execution of each phase using command-line options and a common config file
 *    - Completely non-locking on any table (InnoDB or MyISAM)
 *    - If you cancel the process it will resume were it left
 * 
 * Feel free to email me if you have any questions or need consulting
 */

ini_set('display_errors', 1);
error_reporting(E_ALL);
set_time_limit(0);

define('MASTER_MODE', 0);
define('SLAVE_MODE', 1);
define('DEF_DATABASE', 'percona');
define('DEF_TABLE', 'checksums');

/**
 * Quick Logging facility
 * Singlenton
 */
class Logger {

    static $instance = null;
    private $output_file;
    private $console_output;
    private function logFile($str) {
        $fd = fopen($this->output_file, "a+");
        if ($fd) {
            fwrite($fd, $str);
            fclose($fd);
        }
        return $fd;
    }
    
    public static function setConsoleOutput($bBool) {
        Logger::getInstance()->console_output=$bBool;
    }

    public static function setFile($filename) {
        Logger::getInstance()->output_file = $filename;
        ini_set('error_log', $filename);
    }

    public static function getFile() {
        return Logger::getInstance()->output_file;
    }

    public static function getInstance() {
        if (Logger::$instance == null) {
            Logger::$instance = new Logger();
        }
        return Logger::$instance;
    }

    private function __construct() {
        $this->output_file = "/tmp/" . basename(__FILE__) . ".log";
        if (file_exists($this->output_file)) { // Clean log file
            unlink($this->output_file);
        }
        ini_set('error_log', $this->output_file);
        $this->console_output=false;
    }

    public static function log($msg) {
        $out = sprintf("[%s] %s\n", date('c'), $msg);
        Logger::getInstance()->logFile($out);
        if(Logger::getInstance()->console_output) {
            print $out;
        }
        return $out;
    }

    public static function notice($msg) {
        return Logger::log("NOTICE: $msg");
    }

    public static function debug($msg) {
        return Logger::log("DEBUG: $msg");
    }

    public static function warn($msg) {
        return Logger::log("WARNING: $msg");
    }

    public static function error($msg) {
        if ($msg instanceof Exception) {
            Logger::log("ERROR: " . $msg->getMessage());
            return Logger::log("ERROR: " . $msg->getTraceAsString());
        }
        return Logger::log("ERROR: $msg");
    }

    public static function profiling($msg) {
        return Logger::log("PROFILING: $msg");
    }

}

set_exception_handler(function(Exception $e) {
    Logger::error($e);
    return false;
});
set_error_handler(function( $errno, $errstr, $errfile, $errline, array $errcontext ) {
    Logger::error($errstr);
    return false;
});

class DatabaseAdapter {

    private $PDO_LINK;
    public $CHECKSUM_DATABASE; // Database where checksums are stored
    public $CHECKSUM_TABLE; // Table name where checksums are stored
    private $PREPARED_STATEMENTS;

    const SCHEMA_TABLES_QUERY = "SELECT TABLE_SCHEMA,TABLE_NAME,TABLE_TYPE,ENGINE,TABLE_ROWS,TABLE_COMMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA=:schema";
    const SCHEMA_COLUMNS_QUERY = "SELECT COLUMN_NAME,DATA_TYPE,IS_NULLABLE,COLUMN_DEFAULT,CHARACTER_MAXIMUM_LENGTH,COLUMN_TYPE,COLUMN_KEY,COLUMN_COMMENT FROM information_schema.COLUMNS WHERE TABLE_NAME=:table AND TABLE_SCHEMA=:schema";
    // SQL Constants
    const SQL_STORE_MASTER_RESULT = "REPLACE INTO `:CHECKSUM_DATABASE`.`:CHECKSUM_TABLE`(`db`,`tbl`,`chunk`,`chunk_time`,`chunk_index`,`lower_boundary`,`upper_boundary`,`this_crc`,`this_cnt`,`master_crc`,`master_cnt`,`ts`) VALUES (:db,:tbl,:chunk,:time,:index,:lower,:upper,:crc,:cnt,:crc,:cnt,:ts);";
    const SQL_STORE_SLAVE_RESULT = "UPDATE `:CHECKSUM_DATABASE`.`:CHECKSUM_TABLE` SET `chunk_time`=:time,`this_crc`=:crc,`this_cnt`=:cnt,`ts`=:ts WHERE `db`=:db AND `tbl`=:tbl AND `chunk`=:chunk";
    const SQL_LOAD_BOUNDARIES = "SELECT `chunk_index` as `index`, `lower_boundary` as `lower`,`upper_boundary`  as `upper`,`ts` FROM `:CHECKSUM_DATABASE`.`:CHECKSUM_TABLE` WHERE `db`=:dbase AND `tbl`=:table ORDER BY `chunk`;";
    const SQL_ALLLOAD_BOUNDARIES = "SELECT `db`,`tbl`,`chunk` as `chunk_id`, `chunk_index` as `index`, `lower_boundary` as `lower`,`upper_boundary`  as `upper`,`ts` FROM `:CHECKSUM_DATABASE`.`:CHECKSUM_TABLE` ORDER BY `db`,`tbl`,`chunk`;";
    const SQL_REFRESH_BOUNDARIES = "SELECT `db`,`tbl`,`chunk` as `chunk_id`, `chunk_index` as `index`, `lower_boundary` as `lower`,`upper_boundary`  as `upper`,`ts` FROM (SELECT * FROM `:CHECKSUM_DATABASE`.`:CHECKSUM_TABLE` WHERE ts < DATE_SUB(NOW(), INTERVAL :days DAY) OR this_crc='' ORDER BY RAND() LIMIT :batch ) t ORDER BY db,tbl,chunk;";
    // SQL Checksuming
    const SQL_CHECKSUM_METHOD0 = "SELECT HEX(CRC32(SUM(`row_crc`))) as `crc`,COUNT(*) as `cnt` FROM ( SELECT CRC32(CONCAT(:cols_sql)) as `row_crc` FROM (SELECT :quoted_keys FROM :table_name ORDER BY :quoted_keys LIMIT :lower,:upper ) q JOIN :table_name t ON :join_expr ) x GROUP BY 'block';";
    const SQL_CHECKSUM_METHOD1 = "SELECT HEX(CRC32(SUM(`row_crc`))) as `crc`,COUNT(*) as `cnt` FROM ( SELECT CRC32(CONCAT(:cols_sql)) as `row_crc` FROM :table_name t LIMIT :lower,:upper ) x GROUP BY 'block';";
    const SQL_CHECKSUM_METHOD2 = "SELECT HEX(CRC32(SUM(`row_crc`))) as `crc`,COUNT(*) as `cnt` FROM ( SELECT CRC32(CONCAT(:cols_sql)) as `row_crc` FROM :table_name t WHERE :bound_sql ) x GROUP BY 'block';";
    const SQL_CHECKSUM_METHOD3 = "SELECT HEX(CRC32(SUM(`row_crc`))) as `crc`,COUNT(*) as `cnt` FROM ( SELECT CRC32(CONCAT(:cols_sql)) as `row_crc` FROM :table_name t WHERE :bound_sql AND ( :quoted_keys MOD :skip ) =0 ) x GROUP BY 'block';";
    // Table Template
    const SQL_CREATE_CHECKSUMS = <<<EOT
DROP TABLE IF EXISTS `:CHECKSUM_TABLE`;
CREATE TABLE `:CHECKSUM_TABLE` (
  `db` char(64) NOT NULL,
  `tbl` char(64) NOT NULL,
  `chunk` int(11) NOT NULL,
  `chunk_time` float DEFAULT NULL,
  `chunk_index` varchar(200) DEFAULT NULL,
  `lower_boundary` text,
  `upper_boundary` text,
  `this_crc` char(40) NOT NULL,
  `this_cnt` int(11) NOT NULL,
  `master_crc` char(40) DEFAULT NULL,
  `master_cnt` int(11) DEFAULT NULL,
  `ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`db`,`tbl`,`chunk`),
  KEY `ts_db_tbl` (`ts`,`db`,`tbl`)
) ENGINE=InnoDB;
EOT;
    const SQL_RESET_CHECKSUMS = "UPDATE `:CHECKSUM_DATABASE`.`:CHECKSUM_TABLE` SET `master_crc`=null,`master_cnt`=null ";
    const SQL_REPORT = "SELECT * FROM `:CHECKSUM_DATABASE`.`:CHECKSUM_TABLE` WHERE `master_crc` <> '' AND `master_crc` IS NOT NULL AND ( `this_crc` <> `master_crc` OR `this_cnt` <> `master_cnt`)";

    public function __construct($checksumdb, $checksumtable) {
        $this->CHECKSUM_DATABASE = $checksumdb;
        $this->CHECKSUM_TABLE = $checksumtable;
    }

    public function prepare($sql, array $sql_fragments = null) {
        if ($sql_fragments != null) {
            $sql = $this->sqlPrintF($sql, $sql_fragments);
        }
        return $this->PDO_LINK->prepare($sql);
    }

    public function execute(array $params = null) {
        return $this->PDO_LINK->execute($params);
    }

    public function query($sql, array $sql_fragments = null) {
        if ($sql_fragments != null) {
            $sql = $this->sqlPrintF($sql, $sql_fragments);
        }
        return $this->PDO_LINK->query($sql);
    }

    /**
     * Connects to the database and prepares the session
     * @param string $host
     * @param int $port
     * @param string $user
     * @param string $pass
     * @return PDO 
     * @throws ErrorException
     */
    function connectDB(stdClass $connDef) {
        $dsn = "mysql:host={$connDef->host};port={$connDef->port};";
        $options = array(
            PDO::MYSQL_ATTR_INIT_COMMAND => 'SET NAMES utf8',
        );
        if (isset($connDef->options) && is_array($connDef->options)) {
            $options = array_merge($options, $connDef->options);
        }
        $this->PDO_LINK = new PDO($dsn, $connDef->username, $connDef->password, $options);
        if (!$this->PDO_LINK) {
            throw new ErrorException('Error connecting to DB');
        }
        if (isset($connDef->wait_timeout) && intval($connDef->wait_timeout) > 0) {
            $this->PDO_LINK->query('SET wait_timeout ' . intval($connDef->wait_timeout) . ';');
        } else {
            $this->PDO_LINK->query('SET wait_timeout 15;');
        }
        Logger::notice("Connected to database $dsn");
        $this->PDO_LINK->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $this->PREPARED_STATEMENTS = array();
        $this->PREPARED_STATEMENTS['checksum_master'] = $this->prepare(DatabaseAdapter::SQL_STORE_MASTER_RESULT, array(':CHECKSUM_DATABASE' => $this->CHECKSUM_DATABASE, ':CHECKSUM_TABLE' => $this->CHECKSUM_TABLE));
        $this->PREPARED_STATEMENTS['checksum_slave'] = $this->prepare(DatabaseAdapter::SQL_STORE_SLAVE_RESULT, array(':CHECKSUM_DATABASE' => $this->CHECKSUM_DATABASE, ':CHECKSUM_TABLE' => $this->CHECKSUM_TABLE));
        $this->PREPARED_STATEMENTS['stmt_boundaries'] = $this->prepare(DatabaseAdapter::SQL_LOAD_BOUNDARIES, array(':CHECKSUM_DATABASE' => $this->CHECKSUM_DATABASE, ':CHECKSUM_TABLE' => $this->CHECKSUM_TABLE));
        $this->PREPARED_STATEMENTS['schema_tables'] = $this->prepare(DatabaseAdapter::SCHEMA_TABLES_QUERY);
        $this->PREPARED_STATEMENTS['table_columns'] = $this->prepare(DatabaseAdapter::SCHEMA_COLUMNS_QUERY);
        $this->PREPARED_STATEMENTS['report_query'] = $this->prepare(DatabaseAdapter::SQL_REPORT, array(':CHECKSUM_DATABASE' => $this->CHECKSUM_DATABASE, ':CHECKSUM_TABLE' => $this->CHECKSUM_TABLE));
        $this->PREPARED_STATEMENTS['load_boundaries'] = $this->prepare(DatabaseAdapter::SQL_ALLLOAD_BOUNDARIES, array(':CHECKSUM_DATABASE' => $this->CHECKSUM_DATABASE, ':CHECKSUM_TABLE' => $this->CHECKSUM_TABLE));
        $this->PREPARED_STATEMENTS['refresh_boundaries'] = $this->prepare(DatabaseAdapter::SQL_REFRESH_BOUNDARIES, array(':CHECKSUM_DATABASE' => $this->CHECKSUM_DATABASE, ':CHECKSUM_TABLE' => $this->CHECKSUM_TABLE));
        return $this;
    }

    /**
     * Formats SQL using placeholder substitution 
     * @param string $subject
     * @param array $vars
     * @return string
     */
    function sqlPrintF($subject, array $vars) {
        $search = array_keys($vars);
        $replace = array_values($vars);
        return str_replace($search, $replace, $subject);
    }

    function __get($name) {
        if (!isset($this->PREPARED_STATEMENTS[$name])) {
            throw new ErrorException("Statement $name doesnt exists");
        }
        return $this->PREPARED_STATEMENTS[$name];
    }

    function createResultTable() {
        $this->PDO_LINK->query("use " . $this->CHECKSUM_DATABASE . ";");
        $this->PDO_LINK->query($this->sqlPrintF(DatabaseAdapter::SQL_CREATE_CHECKSUMS, array(':CHECKSUM_TABLE' => $this->CHECKSUM_TABLE)));
    }

    function clearResultTable() {
        $this->PDO_LINK->query($this->sqlPrintF(DatabaseAdapter::SQL_RESET_CHECKSUMS, array(':CHECKSUM_DATABASE' => $this->CHECKSUM_DATABASE, ':CHECKSUM_TABLE' => $this->CHECKSUM_TABLE)));
    }

    function checkDatabase() {
        Logger::notice("Checking Database: " . $this->CHECKSUM_DATABASE . " for Table:" . $this->CHECKSUM_TABLE);
        $tables = TABLE_INFO::getTables($this, $this->CHECKSUM_DATABASE);
        $found = false;
        foreach ($tables as $table) {
            $found = $found || ($table->TABLE_NAME == $this->CHECKSUM_TABLE);
        }
        return $found;
    }

    /**
     * Tries to guess the replicated DBs using the Master Status. If the my.cnf file 
     * is replicated this should contain the master configuration.
     * @return mixed Array of DB names or False on error
     */
    function guessReplicatedDBs() {
        $st = $this->PDO_LINK->query("SHOW MASTER STATUS");
        if (false !== ($binlog = $st->fetchColumn(2))) {
            return explode(",", $binlog);
        }
        return false;
    }

}

class ReplicationHasher {

    private $HASHER;
    private $hasher_hash;
    private $db;
    private $timestamp; // Timestamp of the check (when it was run)
    private $skip; // Skip count
    private $ignore_tables;
    private $incremental_check;
    const INDEX_LIMIT = 'LIMIT';
    const INDEX_PRIMARY = 'PRIMARY';

    static function createHashers(stdClass $config) {
        $DSNs = array_merge($config->slaves->DSN, array($config->master->DSN));
        foreach ($DSNs as $dbconfig) {
            $hasher = new ReplicationHasher($dbconfig, $config->general);
            $hashers[] = $hasher;
        }
        return $hashers;
    }

    private function __construct(stdClass $dbconfig, stdClass $general) {
        $this->db = new DatabaseAdapter($general->database, $general->table);
        $this->db->connectDB($dbconfig);
        $this->skip = $general->record_skip;
        $this->ignore_tables = $general->ignore_tables;
        $this->incremental_check=$general->incremental_check;
    }

    /**
     * Calculates and returns the Hash of a table chunk
     * @param PDOStatement $stmt prepared statement
     * @param array $bound Lower/Upper bounds of the query
     * @return mixed False on error and array with Time, CRC32 and Counts on success
     */
    private function calculateChunkHash($bound) {
        if (!$this->isReady($bound['db'], $bound['tbl'])) {
            Logger::error('Fatal error calculating checksums. Terminating');
            return false;
        }
        $start = microtime(true); // Query execution time
        try {
            // For numeric PK or LIMIT boundaries (Otherwise it doesn't work)
            if ((int) intval($bound['lower']) == $bound['lower'] && (int) intval($bound['upper']) == $bound['upper']) {
                $this->HASHER->bindValue(':lower', (int) intval($bound['lower']), PDO::PARAM_INT);
                $this->HASHER->bindValue(':upper', (int) intval($bound['upper']), PDO::PARAM_INT);
            }
            // Using non-numeric PK
            else {
                $this->HASHER->bindValue(':lower', $bound['lower']);
                $this->HASHER->bindValue(':upper', $bound['upper']);
            }
            $this->HASHER->execute();
        } catch (Exception $e) {
            Logger::error($e);
            die();
        }
        $stop = microtime(true);
        $chunk_time = ($stop - $start);
        if (false === ($row = $this->HASHER->fetch(PDO::FETCH_ASSOC))) {
            Logger::error("Unable to fetch Checksum results. Skipping");
            Logger::debug("SQLErr:" . print_r($this->HASHER->errorInfo(), true));
            Logger::debug("Query:" . $this->HASHER->queryString);
            Logger::debug("Params:" . print_r($bound, true));
            return false;
        }
        return array('time' => $chunk_time, 'crc' => $row['crc'], 'cnt' => $row['cnt']);
    }

    function process($bound) {
        if (in_array("{$bound['db']}.{$bound['tbl']}", $this->ignore_tables)) {
            return;
        }
        if (!$this->isReady($bound['db'], $bound['tbl'])) {
            $table = new TABLE_INFO($this->db);
            $table->init($bound['db'], $bound['tbl']);
            $this->prepareTableHasher($table, $bound['index']);
        }
        $row = $this->calculateChunkHash($bound);
        if (false === $row) {
            return;
        }

        $this->db->checksum_slave->execute(array(
            'db' => $bound['db'],
            'tbl' => $bound['tbl'],
            'chunk' => $bound['chunk_id'],
            'time' => $row['time'],
            'crc' => $row['crc'],
            'cnt' => $row['cnt'],
            'ts' => $this->timestamp
        ));
    }

    /**
     * 
     * @param string $table_name
     * @param array $cols
     * @param array $keys
     * @param int $lower
     * @param int $upper
     * @return mixed FALSE on error, an PDOStatement on success
     */
    private function prepareChecksumChunkLimit($table_name, array $cols, array $keys) {
        $params = array();
        $params[':table_name'] = $table_name;
        $params[':quoted_keys'] = "`" . implode("`,`", $keys) . "`";
        $params[':join_expr'] = implode(" AND ", array_map(function($k) {
                    return sprintf("t.`%s` = q.`%s`", $k, $k);
                }, $keys));
        $params[':cols_sql'] = implode(',', array_map(function($c) {
                    return sprintf("QUOTE(t.`%s`)", $c);
                }, $cols)); // Cache Column sql expression
        // Checksum Queries
        try {
            if (count($keys) > 0) {
                // Slowest method, most compatible
                $this->HASHER = $this->db->prepare(DatabaseAdapter::SQL_CHECKSUM_METHOD0, $params);
            } else {
                // Small optimization method
                $this->HASHER = $this->db->prepare(DatabaseAdapter::SQL_CHECKSUM_METHOD1, $params);
            }
        } catch (Exception $e) {
            Logger::debug($e->getTraceAsString());
            return false;
        }
        Logger::notice("Hasher Ready");
        return $this->HASHER;
    }

    /**
     * 
     * @param type $table_name
     * @param array $cols
     * @param string $keys Primary Keyname
     * @param int $lower
     * @param int $upper
     * @return mixed FALSE on error, an array of checksum on success
     */
    private function prepareChecksumChunkPK($table_name, array $cols, $keys) {
        $params = array();
        $params[':table_name'] = $table_name;
        $params[':quoted_keys'] = "`" . implode("`,`", $keys) . "`";
        $params[':cols_sql'] = implode(',', array_map(function($c) {
                    return sprintf("QUOTE(t.`%s`)", $c);
                }, $cols)); // Cache Column sql expression
        $params[':bound_sql'] = "${params[':quoted_keys']} >= :lower AND ${params[':quoted_keys']} <= :upper";
        $params[':skip'] = $this->skip;
        try {
            if ($this->skip) {
                $this->HASHER = $this->db->prepare(DatabaseAdapter::SQL_CHECKSUM_METHOD3, $params);
            } else {
                $this->HASHER = $this->db->prepare(DatabaseAdapter::SQL_CHECKSUM_METHOD2, $params);
            }
        } catch (Exception $e) {
            Logger::debug($e->getTraceAsString());
            return false;
        }
        Logger::notice("Hasher Ready");
        return $this->HASHER;
    }

    function isReady($db, $tbl) {
        return (($this->hasher_hash == md5($db . $tbl)) && ($this->HASHER != null));
    }

    function resetHasher() {
        $this->HASHER = null;
    }

    /**
     * Calculates CRC32 Hashes for a table and saves them on the Checksums table
     * @param TABLE_INFO $table
     * @return void
     * @throws ErrorException
     */
    function prepareTableHasher(TABLE_INFO $table, $indexType = ReplicationHasher::INDEX_LIMIT) {
        $table_name = $table->getFullName();
        Logger::notice('Processing table: ' . $table_name);
        $cols = $table->getColumnNames();
        $keys = $table->getPrimaryKeys();
        Logger::notice('Columns: ' . implode(',', $cols) . ' PRIMARY KEYS: ' . implode(',', $keys));
        //TODO: Optional parallelize Indexing from Hashing (Hard)
        // Optimization, we load Boundaries for both Master and Slave mode to speed up.
        $this->resetHasher();
        if ($indexType == ReplicationHasher::INDEX_PRIMARY) {
            Logger::notice("Preparing PRIMARY Key Hasher");
            $this->prepareChecksumChunkPK($table_name, $cols, $keys);
        } elseif ($indexType == ReplicationHasher::INDEX_LIMIT) {
            Logger::notice("Preparing LIMIT Key Hasher");
            $this->prepareChecksumChunkLimit($table_name, $cols, $keys);
        } else {
            throw new ErrorException("Unknown Index");
        }
        $this->hasher_hash = md5($table->TABLE_SCHEMA . $table->TABLE_NAME);
    }

    function transferResults($data) {
        $update = $this->db->prepare("UPDATE `:CHECKSUM_DATABASE`.`:CHECKSUM_TABLE` SET `master_crc`=:crc,`master_cnt`=:cnt WHERE `db`=:db AND `tbl`=:tbl AND `chunk`=:chunk  ", array(':CHECKSUM_TABLE' => $this->db->CHECKSUM_TABLE, ':CHECKSUM_DATABASE' => $this->db->CHECKSUM_DATABASE));
        foreach ($data as $row) {
            $update->execute($row);
        }
    }
    function loadBoundaries() {
        if ($this->incremental_check) {
            $this->db->refresh_boundaries->bindValue(':days', 1, PDO::PARAM_INT);
            $this->db->refresh_boundaries->bindValue(':batch', 100, PDO::PARAM_INT);
            $stmt = $this->db->refresh_boundaries;
        } else {
            $stmt = $this->db->load_boundaries;
        }
        $stmt->execute();
        if (false === ($table = $stmt->fetchAll(PDO::FETCH_ASSOC))) {
            print "******** No Indexes Found ********\n\n";
            die();
        }
        return $table;
    }
}

/**
 * Table Information Class
 */
class TABLE_INFO {

    private $db;
    public $TABLE_SCHEMA;
    public $TABLE_NAME;
    public $TABLE_TYPE;
    public $ENGINE;
    public $TABLE_ROWS;
    public $TABLE_COMMENT;
    public $TABLE_COLUMNS;

    /**
     * 
     */
    public function __construct(DatabaseAdapter $db) {
        $this->TABLE_COLUMNS = null;
        $this->db = $db;
    }

    public function init($dbname, $tblname) {
        $this->TABLE_NAME = $tblname;
        $this->TABLE_SCHEMA = $dbname;
    }

    /**
     * Get an Object Array with the Table Schema
     * @param PDO $dbh
     */
    public static function getTables(DatabaseAdapter $dbh, $dbname) {
        if ($dbh->schema_tables->execute(array("schema" => $dbname))) {
            if (false !== ($rows = $dbh->schema_tables->fetchAll(PDO::FETCH_CLASS | PDO::FETCH_PROPS_LATE, __CLASS__, array($dbh)))) {
                return $rows;
            }
        }
        return false;
    }

    public function getColumns() {
        if (empty($this->TABLE_COLUMNS)) {
            if (!$this->db->table_columns->execute(array("schema" => $this->TABLE_SCHEMA, 'table' => $this->TABLE_NAME))) {
                return false;
            }
            if (false === ($this->TABLE_COLUMNS = $this->db->table_columns->fetchAll(PDO::FETCH_ASSOC))) {
                return false;
            }
        }
        return $this->TABLE_COLUMNS;
    }

    public function getColumnNames() {
        $cols = $this->getColumns();
        return array_map(function($o) {
            return ($o['COLUMN_NAME']);
        }, $cols);
    }

    public function getPrimaryKeys() {
        $cols = $this->getColumns();
        return array_map(function($o) {
            return ($o['COLUMN_NAME']);
        }, array_filter($cols, function($o) {
                    return ($o['COLUMN_KEY'] == "PRI");
                }));
    }

    public function getFullName($quoted = true) {
        if ($quoted) {
            return "`" . $this->TABLE_SCHEMA . "`.`" . $this->TABLE_NAME . "`";
        } else {
            return $this->TABLE_SCHEMA . "`" . $this->TABLE_NAME;
        }
    }

    public function getRowCount() {
        return $this->TABLE_ROWS;
    }

}

/**
 * Main Replication Check class
 */
class ReplicationIndexer {

    private $min_block_size; // Min Chunk Block size
    private $max_block_size; // Max Chunk Block size
    private $total_start; // Performance Timer
    private $normal_exit; // Flag to indicate if the exit is normal
    private $ignore_tables; // array of databases.tables to ignore

    /* @var DatabaseAdapter */
    private $db; // Active PDO Database connection
    private $timestamp; // Timestamp of the check (when it was run)
    private $query_log; // Where do we want to store the queries for debugging
    private $force_reset; // Forces a drop an recreate of the CHECKSUMS table
    private $incremental_check; // Optimization (speed) to check only recent chunks since last check

    /**
     * Standard constructor
     * @param stdClass $config
     */

    function __construct(stdClass $config) {
        $this->query_log = $config->general->query_log;
        if (file_exists($this->query_log)) {
            unlink($this->query_log);
        }
        $this->min_block_size = $config->general->min_block_size;
        $this->max_block_size = $config->general->max_block_size;
        $this->skip = $config->general->record_skip;
        $this->force_reset = $config->general->force_reset;
        $this->incremental_check = $config->general->incremental_check;
        $this->db = new DatabaseAdapter($config->general->database, $config->general->table);
        $this->db->table_hasher = null;
        /**
         * Open file for Queries for debugging
         */
        if (file_exists($this->query_log)) {
            unlink($this->query_log);
        }

        $this->ignore_tables = $config->general->ignore_tables;
        $this->total_start = microtime(true);
        register_shutdown_function(array($this, 'finish'));
    }

    /**
     * Parses a validates a config INI file
     * @param string $config_filename
     * @return stdClass
     * @throws ErrorException
     */
    static function loadConfig($config_filename) {
        $config = parse_ini_file($config_filename, true, INI_SCANNER_NORMAL);
        $config = json_decode(json_encode($config));

        if ($config == null) {
            throw new ErrorException('Invalid Config: Cannot parse');
        }



        $v = array();
        parse_str($config->master->DSN, $v);
        $config->master->DSN = json_decode(json_encode($v));
        ;
        if (!isset($config->master) || !isset($config->master->DSN)) {
            throw new ErrorException('Invalid Config: Missing Master section');
        }
        $config->master->DSN->run_mode = MASTER_MODE;

        if (!isset($config->slaves) || empty($config->slaves->DSN)) {
            throw new ErrorException('Invalid Config: Missing Slave section');
        }
        $list = array();
        foreach ($config->slaves->DSN as $str) {
            $v = array();
            parse_str($str, $v);
            if (empty($v)) {
                throw new ErrorException('Invalid Config: Missing Slave DSN: ' . $str);
            }
            $v['run_mode'] = SLAVE_MODE;
            $list[] = $v;
        }
        $config->slaves->DSN = json_decode(json_encode($list));

        //Options
        $options = array(
            'min_block_size' => FILTER_VALIDATE_INT,
            'max_block_size' => FILTER_VALIDATE_INT,
            'record_skip' => FILTER_VALIDATE_INT,
            'force_reset' => FILTER_VALIDATE_BOOLEAN,
            'incremental_check' => FILTER_VALIDATE_BOOLEAN,
            'print_results' => FILTER_VALIDATE_BOOLEAN,
            'email_report' => FILTER_VALIDATE_EMAIL
        );
        foreach ($options as $opt => $mode) {
            $value = $config->general->{$opt};

            $config->general->{$opt} = filter_var($value, $mode);
        }
        // Validate Config
        if (empty($config->general->min_block_size) || intval($config->general->min_block_size) == 0) {
            $config->general->min_block_size = 1000;
        }
        if (empty($config->general->max_block_size) || intval($config->general->max_block_size) == 0) {
            $config->general->max_block_size = 1000000;
        }
        if (empty($config->general->database)) {
            $config->general->database = DEF_DATABASE;
        }
        if (empty($config->general->table)) {
            $config->general->table = DEF_TABLE;
        }
        if ($config->general->force_reset && $config->general->incremental_check) {
            throw new ErrorException("You can't combine options force_reset & incremental_check. Use one or another");
        }
        return $config;
    }

    /**
     * Records the SQL statement on a log file
     * @param string $sql
     * @throws ErrorException
     */
    function logQuery($sql) {
        if (false === ($fd_queries = fopen($this->query_log, 'a+'))) {
            throw new ErrorException('Unable to open ' . $this->query_log);
        }
        // Log Query for debugging and performance purposes
        if ($fd_queries) {
            fwrite($fd_queries, $sql . "\n");
            fclose($fd_queries);
        }
    }

    /**
     * Loads the lower/upper boundary from the checksums table
     * @param TABLE_INFO $table
     * @return array
     */
    function loadTableBoundaries($table) {
        $arr_bound = array();
        Logger::notice('Loading Boundaries...');
        $this->db->stmt_boundaries->execute(array('dbase' => $table->TABLE_SCHEMA, 'table' => $table->TABLE_NAME));
        if (false === ($arr_bound = $this->db->stmt_boundaries->fetchAll(PDO::FETCH_ASSOC))) {
            Logger::error('Unable to load table boundaries');
        }
        return $arr_bound;
    }

    /**
     * Finds lower/upper boundaries for each chunk
     * @param TABLE_INFO $table
     * @param array $arr_bound reference to the pre-existing boundaries
     * @return array completed lower/upper array with boundaries
     * @throws ErrorException
     */
    function indexTable($table, &$arr_bound) {
        // Initialize values for Master process
        $keys = $table->getPrimaryKeys();
        $rowcount = intval($table->getRowCount());
        $page_size = $this->getPageSize($rowcount);
        Logger::notice('Calculating Boundaries (chunk_size: ' . $page_size . ')...');
        if (count($keys) == 1) {
            $arr_bound = $this->getBoundsPK($table->getFullName(), $keys, $rowcount, $page_size, $arr_bound);
        } else {
            $arr_bound = $this->getBoundsLimit($table->getFullName(), $rowcount, $page_size, $arr_bound);
        }
        if ($arr_bound === false) {
            Logger::error('Fatal error occurred getting SQL Bounds');
            throw new ErrorException('Fatal error occurred getting SQL Bounds');
        }
        return $arr_bound;
    }

    function getPageSize($rowcount) {
        return max(array($this->min_block_size, min(array(ceil($rowcount / 10), $this->max_block_size)))); // minimum 1mill records
    }

    /**
     * Calculates CRC32 Hashes for a table and saves them on the Checksums table
     * @param TABLE_INFO $table
     * @return void
     * @throws ErrorException
     */
    function prepareTableHasher(DatabaseAdapter $db, TABLE_INFO $table) {
        $table_name = $table->getFullName();
        if (in_array($table_name, $this->ignore_tables)) {
            Logger::notice('Ignoring table:' . $table_name);
            return;
        }
        Logger::notice('Processing table: ' . $table_name);
        $cols = $table->getColumnNames();
        $keys = $table->getPrimaryKeys();
        $rowcount = intval($table->getRowCount());
        Logger::notice('Columns: ' . implode(',', $cols) . ' PRIMARY KEYS: ' . implode(',', $keys));
        //TODO: Optional parallelize Indexing from Hashing (Hard)
        $table_start = microtime(true); // Table execution time
        // Optimization, we load Boundaries for both Master and Slave mode to speed up.
        $arr_bound = $this->loadTableBoundaries($table);
        $this->db->table_hasher = null;



        Logger::notice('Hashing: ' . $rowcount . ' records on ' . count($arr_bound) . ' chunks' . (($this->incremental_check) ? ' (Incremental mode)' : ''));
        foreach ($arr_bound as $chunk_id => $bound) {
            if ($this->incremental_check && isset($arr_bound['ts'])) {
                // Removed DB loaded bounds to speedup. (check most recent)
                unset($arr_bound[$chunk_id]);
            }
            if (!$db->isReady() && $bound['index'] == 'PRIMARY') {
                $db->prepareChecksumChunkPK($table_name, $cols, $keys);
            } elseif (!$db->isReady() && $bound['index'] == 'LIMIT') {
                $db->prepareChecksumChunkLimit($table_name, $cols, $keys);
            } else {
                throw new ErrorException("Unknown Index");
            }
        }
        return $arr_bound;
    }

    /**
     * Clears previous results to avoid confusion
     */
    function resetResults() {
        if ($this->force_reset) {
            Logger::notice("Creating Checksum table");
            $this->db->createResultTable();
        } elseif (!$this->incremental_check) {
            $this->db->clearResultTable();
        }
    }

    function indexServer($dbconfig) {
        $this->normal_exit = false;
        $this->timestamp = date('Y-m-d H:i:s');
        Logger::notice("Starting");
        $this->db->connectDB($dbconfig);
        if (!$this->db->checkDatabase()) {
            Logger::notice("Checksum Table doesn't exists. Trying to reate it");
            $this->force_reset = true;
        }
        $this->resetResults();
        $replicated_dbs = $this->db->guessReplicatedDBs();
        Logger::notice("Replicated Databases:" . implode(',', $replicated_dbs));
        foreach ($replicated_dbs as $dbname) {
            Logger::notice('Getting tables for:' . $dbname);
            $tables = TABLE_INFO::getTables($this->db, $dbname);
            Logger::notice('Found ' . count($tables) . ' table(s)');
            foreach ($tables as $table) {
                Logger::notice('Indexing ' . $table->getFullName());
                $arr_bound = $this->loadTableBoundaries($table);
                $this->indexTable($table, $arr_bound);
            }
        }
        $this->normal_exit = true;
        $this->finish();
        return;
    }

    /**
     * 
     * @param string $table_name
     * @param array $keys
     * @param int $rowcount
     * @param int $page_size
     * @return mixed FALSE on error, an PDOStatement on success.
     */
    function getBoundsPK($table_name, array $keys, $rowcount, $page_size, $bounds) {
        $start = microtime(true);
        if (count($keys) == 1) {
            $quoted_keys = "`" . implode("", $keys) . "`";
        } else {
            $quoted_keys = "CONCAT(`" . implode("`),QUOTE(`", $keys) . "`)"; // CONCAT is slooooooow....
        }
        $params = array(':primary_key' => $quoted_keys, ':table_name' => $table_name);
        $stmt = $this->db->prepare("SELECT MAX(x) as `upper` FROM ( SELECT :primary_key as x FROM :table_name WHERE :primary_key >= :start ORDER BY :primary_key LIMIT 0,:stop) t;", $params);
        if ($stmt === false) {
            throw new ErrorException('Unable to prepare statement');
        }
        $this->logQuery($stmt->queryString);
        $stmt_limits = $this->db->query("SELECT MIN(:primary_key) as `start_pk`,MAX(:primary_key) as `end_pk` FROM :table_name ", $params);
        if (FALSE === ($limits = $stmt_limits->fetch())) {
            throw new ErrorException("Unable to get PK limits");
        }
        $last_upper = 0;
        if (count($bounds)) {
            $last_boundary = array_pop($bounds);
            $last_upper = ($last_boundary['lower'] != null) ? $last_boundary['lower'] : $limits['start_pk'];
        }
        list($db, $tbl) = explode(".", str_replace('`', '', $table_name));
        $offset = 0;
        while ( $last_upper < $limits['end_pk']) {
            Logger::debug('Scaning PK(' . $quoted_keys . ') from ' . $last_upper . ' of ' . $limits['end_pk']);
            // Note: this works good with auto_increment Primary Keys
            $stmt->bindValue(':start', $last_upper, PDO::PARAM_INT);
            $stmt->bindValue(':stop', $page_size, PDO::PARAM_INT);
            $stmt->execute();
            $row = $stmt->fetch(PDO::FETCH_ASSOC);
            if (false === $row) {
                Logger::error('Unable to get bounds for ' . $last_upper . ':' . $page_size . ' on table ' . $table_name . ' -- ' . $sql);
                $err = $stmt->errorInfo();
                Logger::error($err[2]);
                return false;
            }
            if($row['upper'] == null) {
               // Force complete
                $row['upper']=$limits['end_pk'];
            }
            $bounds[] = array('index' => 'PRIMARY', 'lower' => $last_upper, 'upper' => $row['upper']);
            //Store bounds for reuse

            $this->db->checksum_master->execute(array(
                'db' => $db,
                'tbl' => $tbl,
                'chunk' => count($bounds) - 1,
                'time' => null,
                'index' => 'PRIMARY',
                'lower' => $last_upper,
                'upper' => $row['upper'],
                'crc' => '',
                'cnt' => 0,
                'ts' => $this->timestamp
            ));
            if ($rowcount < ($offset + $page_size)) {
                $page_size = $rowcount - $offset;
            }
            $offset+=$page_size;
            Logger::notice("\t\t" . number_format($offset / $rowcount * 100.0, 2) . "% done...");

            $last_upper = $row['upper']; //adjust offset
        }
        $stop = microtime(true);
        Logger::profiling(sprintf("Index completed in:\t\t\t %0.4f seconds", ($stop - $start)));
        return $bounds;
    }

    /**
     * 
     * @param string $table_name
     * @param array $keys
     * @param int $rowcount
     * @param int $page_size
     * @return mixed FALSE on error, an PDOStatement on success.
     */
    function getBoundsLimit($table_name, $rowcount, $page_size, $bounds) {
        $start = microtime(true);
        //Optimization, dont recalculate all boundaries again, just the last one (insertions).
        $last_upper = 0;
        if (count($bounds)) {
            $last_boundary = array_pop($bounds);
            $last_upper = ($last_boundary['lower'] != null) ? $last_boundary['lower'] : $limits['start_pk'];
        }
        //Store bounds for reuse
        list($db, $tbl) = explode(".", str_replace('`', '', $table_name));

        $star_offset = count($bounds) * $page_size;
        for ($offset = $star_offset; $offset < $rowcount; $offset+=$page_size) {
            if ($rowcount < ($offset + $page_size)) {
                $page_size = $rowcount - $offset;
            }
            Logger::debug('Scaning PK from ' . $offset . ' to ' . ($offset + $page_size));
            //Use Limit instead (slower)
            $bounds[] = array('lower' => $offset, 'upper' => ($offset + $page_size - 1));
            //Store bounds for reuse


            $this->db->checksum_master->execute(array(
                'db' => $db,
                'tbl' => $tbl,
                'chunk' => count($bounds) - 1,
                'time' => null,
                'index' => 'LIMIT',
                'lower' => $offset,
                'upper' => ($offset + $page_size - 1),
                'crc' => '',
                'cnt' => 0,
                'ts' => $this->timestamp
            ));
            Logger::notice("\t\t" . number_format($offset / $rowcount * 100.0, 2) . "% done...");
        }
        $stop = microtime(true);
        Logger::profiling(sprintf("Index completed in:\t\t\t %0.4f seconds", ($stop - $start)));
        return $bounds;
    }

    /**
     * Reports the end run time.
     * @return boolean
     */
    function finish() {
        $total_stop = microtime(true);
        if ($this->normal_exit) {
            Logger::profiling(sprintf("Verification complete in:\t\t\t %0.4f seconds", ($total_stop - $this->total_start)));
        } else {
            Logger::profiling(sprintf("Verification aborted. run-time:\t\t\t %0.4f seconds", ($total_stop - $this->total_start)));
        }

        return false;
    }

    function transferResults($masterConfig, $slaveConfig) {
        $command = 'mysqldump';
        if ($masterConfig->username) {
            $command.=' -u' . $masterConfig->username;
        }
        if ($masterConfig->password) {
            $command.=' -p' . $masterConfig->password;
        }
        if ($masterConfig->host) {
            $command.=' -h' . $masterConfig->host;
        }
        if ($masterConfig->port) {
            $command.=' -P' . $masterConfig->port;
        }

        $command.=' ' . $this->db->CHECKSUM_DATABASE . ' | mysql';
        if ($slaveConfig->username) {
            $command.=' -u' . $slaveConfig->username;
        }
        if ($slaveConfig->password) {
            $command.=' -p' . $slaveConfig->password;
        }
        if ($slaveConfig->host) {
            $command.=' -h' . $slaveConfig->host;
        }
        if ($slaveConfig->port) {
            $command.=' -P' . $slaveConfig->port;
        }
        $command.=' ' . $this->db->CHECKSUM_DATABASE;
        Logger::notice('Transfering Results (CMD: ' . $command . ')');
        system($command, $strout);
        Logger::notice($strout);
    }

}

/**
 * Compares hashes and generates email reports
 */
class ReplicationReporter {

    private $db;
    private $to_email;
    private $subject_email;
    private $header_email;

    public static function Report($config) {
        foreach ($config->slaves->DSN as $dbconfig) {
            $reporter = new ReplicationReporter($dbconfig, $config->general);
            $reporter->emailReport();
        }
    }

    private function __construct($dbconfig, $general) {
        $this->db = new DatabaseAdapter($general->database, $general->table);
        $this->db->connectDB($dbconfig);
        $this->subject_email = 'Replication Check Results';
        $this->header_email = "Replication Check Results from " . $dbconfig->host . ". \nFor more info, see log at: " . Logger::getFile() . "\n\n";
        $this->to_email = $general->email_report;
    }

    function emailReport() {
        $this->db->report_query->execute();
        if (false === ($table = $this->db->report_query->fetchAll(PDO::FETCH_ASSOC))) {
            $checksum = "******** Error while retrieving Results ********\n\n";
        } elseif (count($table) == 0) {
            $checksum = "******** OK: All Records Synched ********\n\n";
        } else {
            $checksum = "******** Error: Unsynched Records Found ********\n\n";
            $checksum .= ReplicationReporter::drawTextTable($table);
        }
        mail($this->to_email, $this->subject_email, $this->header_email . $checksum);
    }

    static function drawTextTable($table) {
        // Work out max lengths of each cell
        $cell_lengths = array();
        foreach ($table as $row) {
            $cell_count = 0;
            foreach ($row as $key => $cell) {
                $cell_length = strlen($cell) + 3;
                $cell_count++;
                if (!isset($cell_lengths[$key]) || $cell_length > $cell_lengths[$key]) {
                    $cell_lengths[$key] = $cell_length;
                }
            }
        }
        // Build header bar
        $bar = '+';
        $header = '|';
        $i = 0;

        foreach ($cell_lengths AS $fieldname => $length) {
            $i++;
            $bar .= str_pad('', $length + 2, '-') . "+";
            $name = $fieldname;
            if (strlen($name) > $length) {
                // crop long headings
                $name = substr($name, 0, $length - 1);
            }

            $header .= ' ' . str_pad($name, $length, ' ', STR_PAD_RIGHT) . " |";
        }

        $output = '';
        $output .= $bar . "\n";
        $output .= $header . "\n";
        $output .= $bar . "\n";

        // Draw rows
        foreach ($table AS $row) {
            $output .= "|";
            foreach ($row AS $key => $cell) {
                $output .= ' ' . str_pad($cell, $cell_lengths[$key], ' ', STR_PAD_RIGHT) . " |";
            }
            $output .= "\n";
        }

        $output .= $bar . "\n";
        return $output;
    }

}
/**
 * Simple option parser
 */
class OptionParser {
    const GETOPT_SHORT=1;
    const GETOPT_LONG=2;
    const GETOPT_OPTIONAL=4;
    const GETOPT_BOOL=8;
    const GETOPT_PARAM=16;
    
    private $header;
    private $validOpts;
    private $helpLines;
    private $pairedOpts;
    private $parsedOpts;
    
    
    function __construct() {
        $this->header="";
        $this->helpLines=array();
        $this->validOpts=array();
        $this->pairedOpts=array();
        $this->parsedOpts=array();
    }
    
    /**
     * Asigns the Help heading
     * @param string $str
     */
    function addHead($str) {
        $this->header=$str;
    }
    
    /**
     * Asigns internal options flags
     * @param string $key
     * @param string $mode
     */
    private function setRuleOptions($key,$mode) {
        if($mode==":") {
            $this->validOpts[$key]|=OptionParser::GETOPT_PARAM;
        }elseif($mode=="::"){
            $this->validOpts[$key]|=OptionParser::GETOPT_PARAM | OptionParser::GETOPT_OPTIONAL;
        }else{
            $this->validOpts[$key] |= OptionParser::GETOPT_BOOL;
        }
        
    }
    
    /**
     * Defines a rule using GetOpt format
     * @param string $optdesc
     * @param string $help
     */
    function addRule($optdesc,$help=null) {
        $short_regex = '(?<short>[A-Za-z0-9])';
        $long_regex='(?<long>[A-Za-z0-9_-]{2,})';
        $matches=array();
        if(preg_match("/^$short_regex(?<mode>[:]{0,2})$/", $optdesc,$matches)){
            $this->validOpts[$matches['short']]=  OptionParser::GETOPT_SHORT;
            $this->setRuleOptions($matches['short'], $matches['mode']);
        }elseif(preg_match("/^(:?$short_regex\|$long_regex)(?<mode>[:]{0,2})$/", $optdesc,$matches)) {
            $this->validOpts[$matches['short']]=  OptionParser::GETOPT_SHORT;
            $this->setRuleOptions($matches['short'], $matches['mode']);
            $this->pairedOpts[$matches['short']]=$matches[1];
            $this->validOpts[$matches['long']]=  OptionParser::GETOPT_LONG;
            $this->setRuleOptions($matches['long'], $matches['mode']);
            $this->pairedOpts[$matches['long']]=$matches[1];
        }elseif(preg_match("/^$long_regex(?<mode>[:]{0,2})$/", $optdesc,$matches)) {
            $this->validOpts[$matches['long']]=  OptionParser::GETOPT_LONG;
            $this->setRuleOptions($matches['long'], $matches['mode']);
            $this->pairedOpts[$matches['long']]=$matches[1];
        }else{
            die("Invalid option $optdesc");
        }
        $this->helpLines[$matches[1]]=$help;
    }
    
    /**
     * Executes the parsing of the options
     * @return mixed This function will return an array of option / argument pairs or FALSE on failure.
     */
    function parse() {
        $stropt = "";
        $lngopt=array();
        foreach($this->validOpts as $key=>$mode){
            if($mode & OptionParser::GETOPT_PARAM){
                $key.=":";
            }
            if($mode & OptionParser::GETOPT_OPTIONAL) {
                $key.=":";
            }
            if($mode & OptionParser::GETOPT_SHORT){
                $stropt .=$key;
            }else{
                $lngopt[]=$key;
            }
        }
        $ret=  getopt($stropt, $lngopt);
        
        foreach($ret as $k=>$v) {
            if(isset($this->pairedOpts[$k])){
                $mode = $this->validOpts[$k];
                if($mode & OptionParser::GETOPT_BOOL) {
                    $this->parsedOpts[$this->pairedOpts[$k]]=true;
                }else{
                    $this->parsedOpts[$this->pairedOpts[$k]]=$v;
                }
            }
        }
        return $ret;
    }
    
    /**
     * Used to retrieve the parsed options
     * @param string $key
     * @return mixed TRUE if Flag is set or the value, otherwise NULL
     */
    function getOption($key) {
        if(!isset($this->parsedOpts[$key])) {
            return null;
        }
        return $this->parsedOpts[$key];
    }
    
    /**
     * Outputs the usage information on the console
     */
    function printUsage() {
        echo $this->header;
        foreach($this->helpLines as $key=>$help) {
            $opts=explode("|", $key);
            $opt= array_shift($opts);
            $line="-";
            if($this->validOpts[$opt] & OptionParser::GETOPT_LONG){
                $line.="-";
            }
             $line.="$key";
            if($this->validOpts[$opt] & OptionParser::GETOPT_PARAM){
                 $line.=" <value>";
            }
            
            if($this->validOpts[$opt] & OptionParser::GETOPT_OPTIONAL){
                $help= "$help (Optional)";
            }
            echo sprintf("\t%-30s%s\n",$line,$help);
        }
        echo "\n\n";
    }
}

//MAIN
if (basename(__FILE__) == basename($argv[0])) {
    
    $parser = new OptionParser();
    $parser->addHead("Usage: {$argv[0]} [ options ]\n");
    try {
        $start = microtime(true);
        $parser->addRule('c|config:', 'Use the <file> as configuration (default: '.basename(__FILE__).'.ini)');
        $parser->addRule('d|debug', 'Turn on console debugging output');
        $parser->addRule('i|index', 'Run the indexer on the tables');
        $parser->addRule('k|hash', 'Run the hasher on the tables');
        $parser->addRule('r|report', 'Run the report generator');
        $parser->addRule('print::', 'Print the report on the screen');

        $parser->parse();
        if($parser->getOption('d|debug')) {
            Logger::setConsoleOutput(true);
        }
        
        // Check Architecture
        if (PHP_INT_MAX < 9223372036854775807) {
            throw new ErrorException("Please use 64bit architecture");
        }
        $lock_path="/var/run/".basename($argv[0]).".pid";
        //First, check to see if the script is already running
        /**
        * Basically, it reads the file, and then tries to posix_kill the pid contained inside. 
        * Note, that when you use a signal of 0 to posix_kill, it merely tells you if the call will succeed (meaning that the process is running).
        **/
        $data = @file_get_contents($lock_path);
        if ($data && posix_kill($data, 0)) {
                print "Unable To Attain Lock, Another Process Is Still Running\n";
                throw new RuntimeException(
                        'Unable To Attain Lock, Another Process Is Still Running'
                );
        }
        file_put_contents($lock_path, posix_getpid());
        if(($configFilename=$parser->getOption('c|config')) === null ){
            $configFilename=basename(__FILE__) . ".ini";
        }
        $config = ReplicationIndexer::loadConfig($configFilename);
        //Logger::notice("Configuration:\n" . print_r($config, true));
        //Indexing Phase
        if($parser->getOption('i|index')) {
            $indexer = new ReplicationIndexer($config);
            $indexer->indexServer($config->master->DSN);
            foreach ($config->slaves->DSN as $dbconfig) {
                $indexer->transferResults($config->master->DSN, $dbconfig);
            }
            $indexer = null;
        }
        //Hashing Phase
        if($parser->getOption('k|hash')) {
            $hashers = ReplicationHasher::createHashers($config);
            $boundaries = $hashers[count($hashers)-1]->loadBoundaries();
            foreach ($boundaries as $i => $bound) {
                foreach ($hashers as $hash) {
                    $hash->process($bound);
                }
                Logger::profiling(sprintf("\t\t... %0.2f%% done", $i / count($boundaries) * 100));
            }

            //Get Access to master
            $masterDB = new DatabaseAdapter($config->general->database, $config->general->table);
            $masterDB->connectDB($config->master->DSN);
            //Load data from Master
            $stmt = $masterDB->query("SELECT `db`,`tbl`,`chunk`,`this_crc` as `crc`,`this_cnt` as `cnt` FROM `:CHECKSUM_DATABASE`.`:CHECKSUM_TABLE`", array(':CHECKSUM_TABLE' => $config->general->table, ':CHECKSUM_DATABASE' => $config->general->database));
            $table = $stmt->fetchAll(PDO::FETCH_ASSOC);
            foreach ($hashers as $hash) {
                $hash->transferResults($table);
            }
        }
        if($parser->getOption('r|report')) {
            ReplicationReporter::Report($config);
        }
        $stop = microtime(true);
        Logger::profiling(sprintf("Check completed in:\t\t\t %0.4f seconds", ($stop - $start)));
    } catch (Exception $e) {
        Logger::error($e);
        //Show Usage
        $parser->printUsage();
    }
}

