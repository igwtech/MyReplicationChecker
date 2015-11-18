<?php

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

namespace Replication;
use Replication\Hasher;
use Replication\DB\MySQL\DatabaseAdapter;
use Replication\DB\TableInfo;
use Replication\Logger;
use \PDO;
use \ErrorException;
use PDOStatement;

class Synchronizer extends Hasher {
    protected $masterdb;
    protected $timeStart;
    protected $dryrun;
    public static function createHashers($masterDB,$config,$dryrun) {
        $hashers=array();
        foreach ($config->slaves->DSN as $dbconfig) {
            $syncher = new Synchronizer($dbconfig, $config->general);
            $syncher->masterdb=$masterDB;
            $syncher->timeStart = microtime(true);
            $syncher->dryrun = $dryrun;
            $hashers[]=$syncher;   
        }
        
        return $hashers;
    } 
    
    public function syncResultsOld() {
        $this->db->report_query->execute();
        if (false === ($results = $this->db->report_query->fetchAll(PDO::FETCH_ASSOC))) {
            Logger::notice("******** Error while retrieving Results ********\n");
        } elseif (count($results) == 0) {
            Logger::notice("******** OK: All Records Synched ********\n");
        } else {
            Logger::notice("******** Running: Unsynched Records Found ********");
            $i=0;
            foreach($results as $chunkData) {
                
                $chunkData['index']=$chunkData['chunk_index'] ;
                $chunkData['upper']=$chunkData['upper_boundary'] ;
                $chunkData['lower']=$chunkData['lower_boundary'] ;
                $chunkData['chunk_id']=$chunkData['chunk'] ;
                Logger::notice("Syncing... ".print_r($chunkData,true));
                $table = new TableInfo($this->db);
                $table->init($chunkData['db'], $chunkData['tbl']);
                $indexes=$table->getPrimaryKeys();
                $cols = implode('`,`',$table->getColumnNames());
                $params =':'. implode(',:',$table->getColumnNames()); //TODO: What if the col-names have spaces???
                $params_update ='';
                foreach($table->getColumnNames() as $itm) {
                    $params_update .= '`'.$itm.'`=:'.$itm.',';
                }
                $params_update=  rtrim($params_update, ',');
                if(count($indexes)==1) {
                    $pkindex=  array_pop($indexes);
                }else{
                    die('Unimplemented - Cannot operate on tables with multiple PK indexes');
                }
                $stmtGet=$this->masterdb->prepare("SELECT `COLS` FROM `DB`.`TBL` WHERE `INDEX` >=:lower AND `INDEX` <=:upper",array('COLS'=>$cols, 'DB'=>$chunkData['db'],'TBL'=>$chunkData['tbl'],'INDEX'=>$pkindex));
                //Logger::debug(print_r($this->masterdb,true).print_r($stmtGet,true) . print_r($chunkData,true));
                Logger::profiling(sprintf("\t\t".$chunkData['tbl']."... %0.2f%% done", $i++ / count($results) * 100));
                $stmtPut=$this->db->prepare("INSERT INTO `DB`.`TBL` (`COLS`) VALUES (PARAMS) ON DUPLICATE KEY UPDATE PARUPD ;",array('PARAMS'=>$params, 'COLS'=>$cols, 'DB'=>$chunkData['db'],'TBL'=>$chunkData['tbl'],'PARUPD'=>$params_update));
                // TODO!: We are not removing from the Slave Rows that do not exists on the MASTER.
                // therefore convergence is not guaranteed.
                
                $this->transferData($stmtGet, $stmtPut,$chunkData);
                $stmtIds=$this->masterdb->prepare("SELECT `INDEX` FROM `DB`.`TBL` WHERE `INDEX` >=:lower AND `INDEX` <=:upper",array('COLS'=>$cols, 'DB'=>$chunkData['db'],'TBL'=>$chunkData['tbl'],'INDEX'=>$pkindex));
                if(false != $stmtIds->execute(array('lower'=>$chunkData['lower_boundary'],'upper'=>$chunkData['upper_boundary']) )) {
                    $masterIds = $stmtIds->fetchAll(PDO::FETCH_COLUMN,0);
                }
                $stmtIds=$this->db->prepare("SELECT `INDEX` FROM `DB`.`TBL` WHERE `INDEX` >=:lower AND `INDEX` <=:upper",array('COLS'=>$cols, 'DB'=>$chunkData['db'],'TBL'=>$chunkData['tbl'],'INDEX'=>$pkindex));
                if(false != $stmtIds->execute(array('lower'=>$chunkData['lower_boundary'],'upper'=>$chunkData['upper_boundary']) )) {
                    $slaveIds = $stmtIds->fetchAll(PDO::FETCH_COLUMN,0);
                }
                $removeIds = array_diff($slaveIds, $masterIds);
                Logger::notice('Removing '.count($removeIds).' records');
                $stmtDelete = $this->db->prepare("DELETE FROM `DB`.`TBL` WHERE `INDEX`=:pkid ",array('COLS'=>$cols, 'DB'=>$chunkData['db'],'TBL'=>$chunkData['tbl'],'INDEX'=>$pkindex));
                foreach($removeIds as $k=>$id) {
                    $stmtDelete->execute(array('pkid'=>$id));
                }
                Logger::debug("Synched chunk to Slave: {$chunkData['db']}.{$chunkData['tbl']} #{$chunkData['chunk']}");
                $this->process($chunkData);
                
            }
        }
        
        
    }
    
    protected function transferData($stmtSource, $stmtTarget, $chunkData) {
        $stmt=$this->db->prepare("SELECT @@GLOBAL.max_allowed_packet; ");
        $stmt->execute();
        $max_packet = $stmt->fetchColumn(0);
        $this->db->query("SET GLOBAL max_allowed_packet=1073741824;" );
        $start=  microtime(true);
        Logger::profiling(sprintf("\t\tDownloading chunk #%d %0.4f secs",$chunkData['chunk'], microtime(true) - $start));
        if(false != $stmtSource->execute(array('lower'=>$chunkData['lower_boundary'],'upper'=>$chunkData['upper_boundary']) )) {
            Logger::profiling(sprintf("\t\tBegin upload chunk #%d %0.4f secs",$chunkData['chunk'], microtime(true) - $this->timeStart));
            while($row = $stmtSource->fetch(PDO::FETCH_ASSOC)){
                //Logger::debug(implode(',',$row));
                if(!$stmtTarget->execute($row)) {
                    Logger::error("Error executing...".$stmtTarget->queryString."\n".print_r($row,true) );
                }
            }
            Logger::profiling(sprintf("\t\tDone chunk #%d %0.4f secs",$chunkData['chunk'], microtime(true) - $start));
        }else{
            return false;
        }
        Logger::profiling(sprintf("\t\tTable Synched %0.4f",microtime(true) - $start));
        $this->db->query("SET GLOBAL max_allowed_packet=$max_packet;" );
        return true;
    }
    
    public function syncResults() {
        ini_set('memory_limit','128M');
        $this->db->report_query->execute();
        if (false === ($results = $this->db->report_query->fetchAll(PDO::FETCH_ASSOC))) {
            Logger::notice("******** Error while retrieving Results ********\n");
        } elseif (count($results) == 0) {
            Logger::notice("******** OK: All Records Synched ********\n");
        } else {
            Logger::notice("******** Running: Unsynched Records Found ********");
            
            $i=0;
            foreach($results as $chunkData) {
                $chunkData['index']=$chunkData['chunk_index'] ;
                $chunkData['upper']=$chunkData['upper_boundary'] ;
                $chunkData['lower']=$chunkData['lower_boundary'] ;
                $chunkData['chunk_id']=$chunkData['chunk'] ;
                $this->processSync($chunkData);
            }
        }
    }
    
    public function prepareTableDelete(TableInfo $table){
        $keys = $table->getPrimaryKeys();
        $cols = $table->getColumnNames();
        $params = array();
        $params[':table_name'] = $table->getFullName();
        $params[':quoted_keys'] = "`" . implode("`,`", $keys) . "`";
        $params[':keys_sql'] = implode(" AND ", array_map(function($k) {
                    return sprintf("`%s` = :%s", $k, $k);
                }, $keys));
        
        $stmt = $this->db->prepare("DELETE FROM :table_name WHERE :keys_sql ",$params);
        return $stmt;
    }
    public function prepareTableCopy(TableInfo $table) {
        $keys = $table->getPrimaryKeys();
        $cols = $table->getColumnNames();
        $params = array();
        $params[':table_name'] = $table->getFullName();
        $params[':quoted_keys'] = "`" . implode("`,`", $keys) . "`";
        $params[':keys_sql'] = implode(" AND ", array_map(function($k) {
                    return sprintf("`%s` = :%s", $k, $k);
                }, $keys));
        $params[':cols_sql'] =implode(", ", array_map(function($k) {
                    return sprintf("`%s`", $k);
                }, $cols));
        $params[':ins_params'] =implode(", ", array_map(function($k) {
                    return sprintf(":%s", $k);
                }, $cols));
        $params[':upd_params'] =implode(", ", array_map(function($k) {
                    return sprintf("`%s`=:%s",$k, $k);
                }, $cols));        
        $stmtGet=$this->masterdb->prepare("SELECT :cols_sql FROM :table_name WHERE :keys_sql ",$params);
        $stmtPut=$this->db->prepare("INSERT INTO :table_name (:cols_sql) VALUES (:ins_params) ON DUPLICATE KEY UPDATE :upd_params ;",$params);
        return array('read'=>$stmtGet,'write'=>$stmtPut);
    }
    
    public function processSync($bound) {
	static $keys;
        if (in_array("{$bound['db']}.{$bound['tbl']}", $this->ignore_tables)) {
            return;
        }
        if(in_array("{$bound['db']}.{$bound['tbl']}",array('support.group_user'))){
            return;
        }
	if($bound['db'] == 'support') return;
	if($bound['db'] == 'webedi30') return;
        if($bound['index'] === Hasher::INDEX_LIMIT){
            return;
        }
        if (!$this->isReady($bound['db'], $bound['tbl'])) {
            $table = new TableInfo($this->masterdb);
            $table->init($bound['db'], $bound['tbl']);
            $this->stmtMaster=$this->prepareTableHasher($this->masterdb, $table, $bound['index']);
            $this->stmtSlave =$this->prepareTableHasher($this->db      , $table, $bound['index']);
            
            $this->stmtDelete = $this->prepareTableDelete($table);
            $this->stmtCopy = $this->prepareTableCopy($table);
            $this->hasher_hash = md5($bound['db']. $bound['tbl']);
            $this->HASHER=true;
	    $keys = $table->getPrimaryKeys();
 	    if(count($keys) !== 1 ) { 
		Logger::error('Multiple Primary keys are not supported');
	//	throw new ErrorException("Error: Multiple primary keys in table {$bound['tbl']}");
		return;
	    }
	  
        }
        Logger::notice('Synchronizing chunk #'.$bound['chunk']);
        $listMaster=$this->retrieveRowHashes($this->stmtMaster,$bound);
        $listSlave =$this->retrieveRowHashes($this->stmtSlave,$bound);
        $diff=$this->calculateDiff($listMaster, $listSlave);
       
        foreach($diff as $i=>$entry) {
            Logger::notice(sprintf('%sing record %d. Mem: %d bytes',$entry['action'],$entry['index'], memory_get_usage()));
	    $keyname = $keys[0];    
            if($entry['action']==='INSERT') {
		
                $this->stmtCopy['read']->execute(array($keyname=>$entry['index']));
                $row = $this->stmtCopy['read']->fetch(PDO::FETCH_ASSOC);
                
                Logger::profiling("Server: ".$this->stmtCopy['write']->server ." SQL : ".$this->stmtCopy['write']->queryString . " ". var_export($row,true));
                    if($row !== false ) $this->stmtCopy['write']->execute($row);
                
            }elseif($entry['action']==='DELETE') {
                Logger::profiling("Server: ".$this->stmtCopy['write']->server ." SQL : ".$this->stmtDelete->queryString . " $keyname=>{$entry['index']} ");
                    $this->stmtDelete->execute(array($keyname=>$entry['index']));
            }
	    $this->masterdb->query('SELECT 1'); // ping master to keep connection alive
            gc_collect_cycles();
        }
        unset($diff);
        unset($listMaster);
        unset($listSlave);
    }
   
    /**
	* Runs the OS diff tool between the temporary files to get the different rows.
	**/ 
    public function calculateDiff($master,$slave) {
        $output=array();
        foreach($slave as $key =>$hash) {
            if(!isset($master[$key])) {
                $output[]=array('action'=>'DELETE','index'=> $key); //REMOVE
            }
        }
        foreach ($master as $key=>$hash) {
            if(!isset($slave[$key]) || ($slave[$key] !== $hash)) {
                $output[] = array('action'=>'INSERT','index'=> $key); //UPDATE/INSERT
            }
        }
        
        return $output;
    }
    /**
     * Calculates and returns a file with all Indexes and Hashes per row bettwen 
     * @param PDOStatement $hasher prepared statement
     * @param array $bound Lower/Upper bounds of the query
     * @return mixed False on error and array with Time, CRC32 and Counts on success
     */
    private function retrieveRowHashes(PDOStatement $hasher, $bound) {
        //if (!$this->isReady($bound['db'], $bound['tbl'])) {
        //    Logger::error('Fatal error calculating checksums. Terminating');
        //    return false;
        //}
        $start = microtime(true); // Query execution time
        try {
            // For numeric PK or LIMIT boundaries (Otherwise it doesn't work)
            if ((int) intval($bound['lower']) == $bound['lower'] && (int) intval($bound['upper']) == $bound['upper']) {
                $hasher->bindValue(':lower', (int) intval($bound['lower']), PDO::PARAM_INT);
                $hasher->bindValue(':upper', (int) intval($bound['upper']), PDO::PARAM_INT);
            }
            // Using non-numeric PK
            else {
                $hasher->bindValue(':lower', $bound['lower']);
                $hasher->bindValue(':upper', $bound['upper']);
            }
            $hasher->execute();
        } catch (Exception $e) {
            Logger::error($e);
            die();
        }
        $tmp=array();
        while (false !== ($row = $hasher->fetch(PDO::FETCH_NUM))) {
            list($key,$value)=$row;
            $tmp[$key]=$value;
	    gc_collect_cycles();
        }
        $stop = microtime(true);
        $chunk_time = ($stop - $start);
        Logger::profiling('List retrieved in:'.$chunk_time);
	gc_collect_cycles();
        return $tmp;
    }
    /**
     * Calculates CRC32 Hashes for a table and saves them on the Checksums table
     * @param TableInfo $table
     * @return void
     * @throws ErrorException
     */
    function prepareTableHasher(DatabaseAdapter $db,TableInfo $table, $indexType = Hasher::INDEX_LIMIT) {
        $table_name = $table->getFullName();
        Logger::notice('Processing table: ' . $table_name);
        $cols = $table->getColumnNames();
        $keys = $table->getPrimaryKeys();
        Logger::notice('Columns: ' . implode(',', $cols) . ' PRIMARY KEYS: ' . implode(',', $keys));
        //TODO: Optional parallelize Indexing from Hashing (Hard)
        // Optimization, we load Boundaries for both Master and Slave mode to speed up.
        if ($indexType == Hasher::INDEX_PRIMARY) {
            Logger::notice("Preparing PRIMARY Key Hasher");
            $ret=$this->prepareChecksumRowPK($db,$table_name, $cols, $keys);
        } elseif ($indexType == Hasher::INDEX_LIMIT) {
            Logger::notice("Preparing LIMIT Key Hasher");
            $ret=$this->prepareChecksumRowLimit($db,$table_name, $cols, $keys);
        } else {
            throw new ErrorException("Unknown Index");
        }
        return $ret;
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
    private function prepareChecksumRowLimit(DatabaseAdapter $db,$table_name, array $cols, array $keys) {
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
                $HASHER = $db->prepare(DatabaseAdapter::SQL_SYNC_METHOD0, $params);
            } else {
                // Small optimization method
                $HASHER = $db->prepare(DatabaseAdapter::SQL_SYNC_METHOD1, $params);
            }
        } catch (Exception $e) {
            Logger::debug($e->getTraceAsString());
            return false;
        }
        Logger::notice("Hasher Ready");
        return $HASHER;
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
    private function prepareChecksumRowPK(DatabaseAdapter $db,$table_name, array $cols, $keys) {
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
                $HASHER = $db->prepare(DatabaseAdapter::SQL_SYNC_METHOD3, $params);
            } else {
                $HASHER = $db->prepare(DatabaseAdapter::SQL_SYNC_METHOD2, $params);
            }
        } catch (Exception $e) {
            Logger::debug($e->getTraceAsString());
            return false;
        }
        Logger::notice("Hasher Ready");
        return $HASHER;
    }
    
    
    
}
