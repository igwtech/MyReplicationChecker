<?php

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

namespace Replication\DB\MySQL;
use Replication\Logger;
use Replication\DB\TableInfo;
use PDO;

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
    const SQL_LOAD_BOUNDARIES = "SELECT `db`,`tbl`,`chunk` as `chunk_id`, `chunk_index` as `index`, `lower_boundary` as `lower`,`upper_boundary`  as `upper`,`ts` FROM `:CHECKSUM_DATABASE`.`:CHECKSUM_TABLE` WHERE `db`=:dbase AND `tbl`=:table ORDER BY `chunk`;";
    const SQL_LOAD_SINGLEBOUNDARY = "SELECT `db`,`tbl`,`chunk` as `chunk_id`, `chunk_index` as `index`, `lower_boundary` as `lower`,`upper_boundary`  as `upper`,`ts` FROM `:CHECKSUM_DATABASE`.`:CHECKSUM_TABLE` WHERE `db`=:dbase AND `tbl`=:table AND `chunk`=:chunk;";
    const SQL_ALLLOAD_BOUNDARIES = "SELECT `db`,`tbl`,`chunk` as `chunk_id`, `chunk_index` as `index`, `lower_boundary` as `lower`,`upper_boundary`  as `upper`,`ts` FROM `:CHECKSUM_DATABASE`.`:CHECKSUM_TABLE` ORDER BY `db`,`tbl`,`chunk`;";
    const SQL_ALLLOAD_BOUNDARIES_SYNC = "SELECT `db`,`tbl`,`chunk` as `chunk_id`, `chunk_index` as `index`, `lower_boundary` as `lower`,`upper_boundary`  as `upper`,`ts` FROM `:CHECKSUM_DATABASE`.`:CHECKSUM_TABLE` WHERE this_cnt<>master_cnt OR this_crc <> master_crc ORDER BY `db`,`tbl`,`chunk`;";
    const SQL_REFRESH_BOUNDARIES = "SELECT `db`,`tbl`,`chunk` as `chunk_id`, `chunk_index` as `index`, `lower_boundary` as `lower`,`upper_boundary`  as `upper`,`ts` FROM (SELECT * FROM `:CHECKSUM_DATABASE`.`:CHECKSUM_TABLE` WHERE ts < DATE_SUB(NOW(), INTERVAL :days DAY) OR this_crc='' ORDER BY RAND() LIMIT :batch ) t ORDER BY db,tbl,chunk;";
    // SQL Checksuming
    const SQL_CHECKSUM_METHOD0 = "SELECT HEX(CRC32(SUM(`row_crc`))) as `crc`,COUNT(*) as `cnt` FROM ( SELECT CRC32(CONCAT(:cols_sql)) as `row_crc` FROM (SELECT :quoted_keys FROM :table_name ORDER BY :quoted_keys LIMIT :lower,:upper ) q JOIN :table_name t ON :join_expr ) x GROUP BY 'block';";
    const SQL_CHECKSUM_METHOD1 = "SELECT HEX(CRC32(SUM(`row_crc`))) as `crc`,COUNT(*) as `cnt` FROM ( SELECT CRC32(CONCAT(:cols_sql)) as `row_crc` FROM :table_name t LIMIT :lower,:upper ) x GROUP BY 'block';";
    const SQL_CHECKSUM_METHOD2 = "SELECT HEX(CRC32(SUM(`row_crc`))) as `crc`,COUNT(*) as `cnt` FROM ( SELECT CRC32(CONCAT(:cols_sql)) as `row_crc` FROM :table_name t WHERE :bound_sql ) x GROUP BY 'block';";
    const SQL_CHECKSUM_METHOD3 = "SELECT HEX(CRC32(SUM(`row_crc`))) as `crc`,COUNT(*) as `cnt` FROM ( SELECT CRC32(CONCAT(:cols_sql)) as `row_crc` FROM :table_name t WHERE :bound_sql AND ( :quoted_keys MOD :skip ) =0 ) x GROUP BY 'block';";
    
    // SQL Sync Checksum
    const SQL_SYNC_METHOD0 = "SELECT :quoted_keys,HEX(CRC32(CONCAT(:cols_sql))) as `row_crc` FROM (SELECT :quoted_keys FROM :table_name ORDER BY :quoted_keys LIMIT :lower,:upper ) q JOIN :table_name t ON :join_expr ";
    const SQL_SYNC_METHOD1 = "SELECT :quoted_keys,HEX(CRC32(CONCAT(:cols_sql))) as `row_crc` FROM :table_name t LIMIT :lower,:upper";
    const SQL_SYNC_METHOD2 = "SELECT :quoted_keys,HEX(CRC32(CONCAT(:cols_sql))) as `row_crc` FROM :table_name t WHERE :bound_sql ";
    const SQL_SYNC_METHOD3 = "SELECT :quoted_keys,HEX(CRC32(CONCAT(:cols_sql))) as `row_crc` FROM :table_name t WHERE :bound_sql AND ( :quoted_keys MOD :skip ) =0 ";
    
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

    protected $server;
    
    public function __construct($checksumdb, $checksumtable) {
        $this->CHECKSUM_DATABASE = $checksumdb;
        $this->CHECKSUM_TABLE = $checksumtable;
    }

    public function prepare($sql, array $sql_fragments = null) {
        if ($sql_fragments != null) {
            $sql = $this->sqlPrintF($sql, $sql_fragments);
        }
        $stmt= $this->PDO_LINK->prepare($sql);
        $stmt->server = $this->server;
        return $stmt;
    }

    public function execute(array $params = null) {
        $stmt= $this->PDO_LINK->execute($params);
        $stmt->server = $this->server;
        return $stmt;
    }

    public function query($sql, array $sql_fragments = null) {
        if ($sql_fragments != null) {
            $sql = $this->sqlPrintF($sql, $sql_fragments);
        }
        $stmt= $this->PDO_LINK->query($sql);
        $stmt->server = $this->server;
        return $stmt;
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
    function connectDB(\stdClass $connDef) {
        $dsn = "mysql:host={$connDef->host};port={$connDef->port};dbname=".$this->CHECKSUM_DATABASE;
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
        $this->server=$dsn;
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
        $this->PREPARED_STATEMENTS['stmt_singlebound'] = $this->prepare(DatabaseAdapter::SQL_LOAD_SINGLEBOUNDARY, array(':CHECKSUM_DATABASE' => $this->CHECKSUM_DATABASE, ':CHECKSUM_TABLE' => $this->CHECKSUM_TABLE));
        $this->PREPARED_STATEMENTS['load_sync'] = $this->prepare(DatabaseAdapter::SQL_ALLLOAD_BOUNDARIES_SYNC, array(':CHECKSUM_DATABASE' => $this->CHECKSUM_DATABASE, ':CHECKSUM_TABLE' => $this->CHECKSUM_TABLE));
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
        $tables = TableInfo::getTables($this, $this->CHECKSUM_DATABASE);
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
