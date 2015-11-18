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
date_default_timezone_set('UTC');
ini_set('display_errors', 1);
error_reporting(E_ALL);
set_time_limit(60*15);

include_once __DIR__.'/../src/bootstrap.php';
use Replication\Logger;
use Replication\OptionParser;
use Replication\ConfigUtil;
use Replication\Hasher;
use Replication\Indexer;
use Replication\Reporter;
use Replication\Synchronizer;
use Replication\DB\MySQL\DatabaseAdapter;

//MAIN
function main() {  
    //Enable Garbage Collection
    gc_enable();
    $parser = new OptionParser();
    $appName=(isset($argv[0]))?$argv[0]:APP_NAME;
    $parser->addHead("Usage: {$appName} [ options ]\n");
    try {
        $start = microtime(true);
        $parser->addRule('c|config:', 'Use the <file> as configuration (default: config.ini)');
        $parser->addRule('d|debug', 'Turn on console debugging output');
        $parser->addRule('i|index', 'Run the indexer on the tables');
        $parser->addRule('k|hash', 'Run the hasher on the tables');
        $parser->addRule('r|report', 'Run the report generator');
        $parser->addRule('s|sync', 'Run the data synchronizer');
        $parser->addRule('boundary:','Boundary spec to Hash an specific boundary. ex --boundary="db=db1&tbl=table1&chunk=0"');
        $parser->addRule('print', 'Print the report on the screen');
        $parser->addRule('dryrun', 'Print Queries instead of running them during the Synchronization phase');

        $parser->parse();
        if($parser->getOption('d|debug')) {
            Logger::setConsoleOutput(true);
        }
        
        // Check Architecture
        if (PHP_INT_MAX < 9223372036854775807) {
            ///throw new \ErrorException("Please use 64bit architecture (recommended)");
            Logger::warn("Please use 64bit architecture (recommended)");
            print "Please use 64bit architecture (recommended)\n";
        }
        
	$lock_path="/var/run/$appName.pid";
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
        @\file_put_contents($lock_path, posix_getpid());        
        
        if(($configFilename=$parser->getOption('c|config')) === null ){
            $configFilename= "config.ini";
        }
        $config = ConfigUtil::loadConfig($configFilename);

        if($parser->getOption('i|index') || $parser->getOption('k|hash') || $parser->getOption('r|report') || $parser->getOption('s|sync')) {
            Logger::notice("Configuration:\n" . print_r($config, true));
        }else{
            $parser->printUsage();
            die();
        }
        //Indexing Phase
        if($parser->getOption('i|index')) {
            Logger::notice("Indexing DB");
            $indexer = new Indexer($config);
            $indexer->indexServer($config->master->DSN);
            foreach ($config->slaves->DSN as $dbconfig) {
                $indexer->transferResults($config->master->DSN, $dbconfig);
		gc_collect_cycles();
            }
            $indexer = null;
        }
	gc_collect_cycles();
        //Hashing Phase
        if($parser->getOption('k|hash')) {
            Logger::notice("Hashing DB");
            $hashers = Hasher::createHashers($config);
            Logger::notice("Server Hashers Created");
            if(($bounddef=$parser->getOption('boundary')) !== null) {
                Logger::notice("CHECKING BOUNDARY: ".$bounddef);
                parse_str($bounddef, $oBoundDef);
                $boundaries =$hashers['master']->loadSingleBoundary($oBoundDef);
            }else{
                $boundaries = $hashers['master']->loadBoundaries();
                Logger::notice("Master boundaries loaded");
            }
            
            Logger::notice("Hashing start....");
            foreach($boundaries as $i => $bound) {
                foreach($hashers as $hash) {
                    $hash->process($bound);
		    gc_collect_cycles();
                }
                Logger::profiling(sprintf("\t\t".$bound['tbl']."... %0.2f%% done", $i / count($boundaries) * 100));
            }
            Logger::notice("Hashing end....");
	    $hashers=null;
	    $boundaries=null;
	}
	gc_collect_cycles();
        //Reporting Phase
        if($parser->getOption('r|report')) {
            //Get Access to master
            Logger::notice("Accessing Master....");
            $masterDB = new DatabaseAdapter($config->general->database, $config->general->table);
            $masterDB->connectDB($config->master->DSN);
            //Load data from Master
            Logger::notice("Loading data from Master....");
            $stmt = $masterDB->query("SELECT `db`,`tbl`,`chunk`,`this_crc` as `crc`,`this_cnt` as `cnt` FROM `:CHECKSUM_DATABASE`.`:CHECKSUM_TABLE`", array(':CHECKSUM_TABLE' => $config->general->table, ':CHECKSUM_DATABASE' => $config->general->database));
            $table = $stmt->fetchAll(PDO::FETCH_ASSOC);
            Logger::notice("Loaded ".count($table)." records from Master....");            
	    $hashers = Hasher::createHashers($config);
            $i=0;
            foreach ($hashers as $hash) {
                Logger::notice("Transfering results to slave#".($i++)."...");
                $hash->transferResults($table);
		gc_collect_cycles();
            }
            $hashers=null;
            $table=null;
            $masterDB=null;
        
            Reporter::Report($config);
        }
        gc_collect_cycles();
        if($parser->getOption('s|sync')) {
            //Get Access to master
            Logger::notice("Accessing Master....");
            $masterDB = new DatabaseAdapter($config->general->database, $config->general->table);
            $masterDB->connectDB($config->master->DSN);
            //Load data from Master
            Logger::notice("Loading data from Master....");
            $hashers= Synchronizer::createHashers($masterDB,$config,$parser->getOption('dryrun'));
            foreach($hashers as $hash) {
                $hash->syncResults();
		gc_collect_cycles();
            }
        }
        
        $config=null;
	gc_collect_cycles();
        $stop = microtime(true);
        Logger::profiling(sprintf("Process completed in:\t\t\t %0.4f seconds", ($stop - $start)));
    } catch (Exception $e) {
        Logger::error($e);
        //Show Usage
        $parser->printUsage();
    }
}

main();