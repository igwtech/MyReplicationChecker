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

/**
 * Quick Logging facility
 * Singleton Class
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

