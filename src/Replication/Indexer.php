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

/**
 * Main Replication Check class
 */
class Indexer {

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
     * @param \stdClass $config
     */

    function __construct(\stdClass $config) {
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
     * Records the SQL statement on a log file
     * @param string $sql
     * @throws ErrorException
     */
    function logQuery($sql) {
        if (false === ($fd_queries = fopen($this->query_log, 'a+'))) {
            throw new \ErrorException('Unable to open ' . $this->query_log);
        }
        // Log Query for debugging and performance purposes
        if ($fd_queries) {
            fwrite($fd_queries, $sql . "\n");
            fclose($fd_queries);
        }
    }

    /**
     * Loads the lower/upper boundary from the checksums table
     * @param TableInfo $table
     * @return array
     */
    function loadTableBoundaries($table) {
        $arr_bound = array();
        Logger::notice('Loading Boundaries...');
        $this->db->stmt_boundaries->execute(array('dbase' => $table->TABLE_SCHEMA, 'table' => $table->TABLE_NAME));
        if (false === ($arr_bound = $this->db->stmt_boundaries->fetchAll(\PDO::FETCH_ASSOC))) {
            Logger::error('Unable to load table boundaries');
        }
        return $arr_bound;
    }

    /**
     * Finds lower/upper boundaries for each chunk
     * @param TableInfo $table
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
        if (count($keys) == 1 ) {
            $arr_bound = $this->getBoundsPK($table->getFullName(), $keys, $rowcount, $page_size, $arr_bound);
        } else {
            $arr_bound = $this->getBoundsLimit($table->getFullName(), $rowcount, $page_size, $arr_bound);
        }
        if ($arr_bound === false) {
            Logger::error('Fatal error occurred getting SQL Bounds');
            throw new \ErrorException('Fatal error occurred getting SQL Bounds');
        }
        return $arr_bound;
    }

    function getPageSize($rowcount) {
        return max(array($this->min_block_size, min(array(ceil($rowcount / 10), $this->max_block_size)))); // minimum 1mill records
    }

    /**
     * Calculates CRC32 Hashes for a table and saves them on the Checksums table
     * @param TableInfo $table
     * @return void
     * @throws ErrorException
     */
    function prepareTableHasher(DatabaseAdapter $db, TableInfo $table) {
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
                throw new \ErrorException("Unknown Index");
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
            $tables = TableInfo::getTables($this->db, $dbname);
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
            throw new \ErrorException('Unable to prepare statement');
        }
        $this->logQuery($stmt->queryString);
        $stmt_limits = $this->db->query("SELECT MIN(:primary_key) as `start_pk`,MAX(:primary_key) as `end_pk` FROM :table_name ", $params);
        if (FALSE === ($limits = $stmt_limits->fetch())) {
            throw new \ErrorException("Unable to get PK limits");
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
            $stmt->bindValue(':start', $last_upper, \PDO::PARAM_INT);
            $stmt->bindValue(':stop', $page_size, \PDO::PARAM_INT);
            $stmt->execute();
            $row = $stmt->fetch(\PDO::FETCH_ASSOC);
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
        $strout='';
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
