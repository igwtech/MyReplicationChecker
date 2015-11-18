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

namespace Replication;
use Replication\DB\MySQL\DatabaseAdapter;
use Replication\DB\TableInfo;
use Replication\Logger;
use \PDO;
use \ErrorException;

class Hasher {

    protected $HASHER;
    protected $hasher_hash;
    protected $db;
    protected $timestamp; // Timestamp of the check (when it was run)
    protected $skip; // Skip count
    protected $ignore_tables;
    protected $incremental_check;
    protected $refresh_days;
    protected $refresh_batch;
    const INDEX_LIMIT = 'LIMIT';
    const INDEX_PRIMARY = 'PRIMARY';

    static function createHashers(\stdClass $config) {
        foreach ($config->slaves->DSN as $i=>$dbconfig) {
            $hasher = new Hasher($dbconfig, $config->general);
            $hashers['slave_'.$i] = $hasher;
        }
        $hasher=new Hasher($config->master->DSN,$config->general);
        $hashers['master'] = $hasher;
        return $hashers;
    }

    /**
     * 
     * @param \Replication\\stdClass $dbconfig
     * @param \Replication\\stdClass $general
     */
    protected function __construct(\stdClass $dbconfig, \stdClass $general) {
        $this->db = new DatabaseAdapter($general->database, $general->table);
        $this->db->connectDB($dbconfig);
        $this->skip = $general->record_skip;
        $this->ignore_tables = $general->ignore_tables;
        $this->incremental_check=$general->incremental_check;
        $this->refresh_batch=$general->incremental_batchsize;
        $this->refresh_days=$general->expire_days;
    }

    /**
     * Calculates and returns the Hash of a table chunk
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

    /**
     * 
     * @param arrray $bound
     * @return void
     */
    public function process($bound) {
        if (in_array("{$bound['db']}.{$bound['tbl']}", $this->ignore_tables)) {
            return;
        }
        if (!$this->isReady($bound['db'], $bound['tbl'])) {
            $table = new TableInfo($this->db);
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
     * @param string $table_name
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
     * @param TableInfo $table
     * @return void
     * @throws ErrorException
     */
    function prepareTableHasher(TableInfo $table, $indexType = Hasher::INDEX_LIMIT) {
        $table_name = $table->getFullName();
        Logger::notice('Processing table: ' . $table_name);
        $cols = $table->getColumnNames();
        $keys = $table->getPrimaryKeys();
        Logger::notice('Columns: ' . implode(',', $cols) . ' PRIMARY KEYS: ' . implode(',', $keys));
        //TODO: Optional parallelize Indexing from Hashing (Hard)
        // Optimization, we load Boundaries for both Master and Slave mode to speed up.
        $this->resetHasher();
        if ($indexType == Hasher::INDEX_PRIMARY) {
            Logger::notice("Preparing PRIMARY Key Hasher");
            $this->prepareChecksumChunkPK($table_name, $cols, $keys);
        } elseif ($indexType == Hasher::INDEX_LIMIT) {
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
            $this->db->refresh_boundaries->bindValue(':days', $this->refresh_days, PDO::PARAM_INT);
            $this->db->refresh_boundaries->bindValue(':batch', $this->refresh_batch, PDO::PARAM_INT);
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
    
    function loadSingleBoundary($boundDef) {
        //Logger::debug(print_r($boundDef,true));
        $this->db->stmt_singlebound->bindValue(':dbase', $boundDef['db'],PDO::PARAM_STR);
        $this->db->stmt_singlebound->bindValue(':table', $boundDef['tbl'],PDO::PARAM_STR);
        $this->db->stmt_singlebound->bindValue(':chunk', $boundDef['chunk'],PDO::PARAM_INT);
        $stmt = $this->db->stmt_singlebound;
        $stmt->execute();
        if ((false === ($table = $stmt->fetchAll(PDO::FETCH_ASSOC))) || count($table) == 0) {
            print "******** No Indexes Found ********\n\n";
            die();
        }
        //var_dump($table);
        return $table;
    }
}
