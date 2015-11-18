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
 * Compares hashes and generates email reports
 */
class Reporter {

    private $db;
    private $to_email;
    private $subject_email;
    private $header_email;

    public static function Report($config) {
        foreach ($config->slaves->DSN as $dbconfig) {
            $reporter = new Reporter($dbconfig, $config->general);
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
            $checksum .= Reporter::drawTextTable($table);
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
