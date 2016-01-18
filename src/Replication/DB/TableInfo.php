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


namespace Replication\DB;
use Replication\DB\MySQL\DatabaseAdapter;
/**
 * Table Information Class
 */
class TableInfo {

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
            if (false !== ($rows = $dbh->schema_tables->fetchAll(\PDO::FETCH_CLASS | \PDO::FETCH_PROPS_LATE, __CLASS__, array($dbh)))) {
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
            if (false === ($this->TABLE_COLUMNS = $this->db->table_columns->fetchAll(\PDO::FETCH_ASSOC))) {
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

    protected function getKeyColumns($onlyAutoIncrement=false) {
        $cols = $this->getColumns();
        return array_map(function($o) {
            return ($o['COLUMN_NAME']);
        }, array_filter($cols, function($o) use ($onlyAutoIncrement){
                if($onlyAutoIncrement) {
                    return ($o['EXTRA'] == "auto_increment");
                }
                return ($o['COLUMN_KEY'] == "PRI");
            }));
    }
    public function getPrimaryKeys() {
        $keys = $this->getKeyColumns();
        $keys2 = $this->getKeyColumns(true);
        if(count($keys2)==1) {
            return $keys2;
        }else{
            return $keys;
        }
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
