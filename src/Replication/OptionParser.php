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
use Replication\Logger;
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

