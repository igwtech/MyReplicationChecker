<?php

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

namespace Replication;

class ConfigUtil {
    const VALIDATE_BOOL=FILTER_VALIDATE_BOOL;
    const VALIDATE_INT=FILTER_VALIDATE_INT;
    const VALIDATE_FLOAT=FILTER_VALIDATE_FLOAT;
    const VALIDATE_IP=FILTER_VALIDATE_IP;
    const VALIDATE_PATHDIR='ConfigUtil::ValidatPathDir';
    const VALIDATE_PATHFILE='ConfigUtil::ValidatPathFile';
    const VALIDATE_EMAIL=FILTER_VALIDATE_EMAIL;
    const VALIDATE_URL=FILTER_VALIDATE_URL;
    const VALIDATE_TABLESARRAY='ConfigUtil::ValidateTableArray';
    const VALIDATE_DSN='ConfigUtil::ValidateDSN';
    const VALIDATE_DSNARRAY='ConfigUtil::ValidateDSNArray';
    
    static function ValidatPathDir($variable,$options) {
        
        $found= file_exists(dirname($variable));
        if(!$found && !empty($options['default'])) {
            return $options['default'];
        }else{
            throw new \ErrorException(__FUNCTION__." Failed");
        }
        
    }
    static function ValidatPathFile($variable,$options) {
        
    }
    static function ValidateTableArray($variable,$options) {
        
    }
    static function ValidateDSN($variable,$options){
        
    }
    static function ValidateDSNArray($variable,$options) {
        
    }
    
    static $configDef=array(
        //Section
        'general'=>array(
            //Options
            'print_results'=>array('validation'=> ConfigUtil::VALIDATE_BOOL,'default'=>false),
            'email_report'=>array('validation'=>ConfigUtil::VALIDATE_EMAIL,'default'=>''),
            'query_log'=>array('validation'=>ConfigUtil::VALIDATE_PATHFILE,'default'=>''),
            'log'=>array('validation'=>ConfigUtil::VALIDATE_PATHFILE),
            'min_block_size'=>array('validation'=>ConfigUtil::VALIDATE_INT,'default'=>100000),
            'max_block_size'=>array('validation'=>ConfigUtil::VALIDATE_INT,'default'=>500000),
            'record_skip'=>array('validation'=>ConfigUtil::VALIDATE_INT,'default'=>0),
            'ignore_tables'=>array('validation'=>ConfigUtil::VALIDATE_TABLESARRAY,'default'=>array()),
            'force_reset'=>array('validation'=>ConfigUtil::VALIDATE_BOOL,'default'=>false),
            'incremental_check'=>array('validation'=>ConfigUtil::VALIDATE_BOOL,'default'=>false),
            'expire_days'=>array('validation'=>ConfigUtil::VALIDATE_INT,'default'=>1),
            'incremental_batchsize'=>array('validation'=>ConfigUtil::VALIDATE_INT,'default'=>100)
        ),
        'master'=>array(
            'DSN'=>array('validation'=>ConfigUtil::VALIDATE_DSN,'default'=>array('host'=>'localhost','port'=>3306,'username'=>'root','password'=>null))
        ),
        'slaves'=>array(
            'DSN'=>array('validation'=>ConfigUtil::VALIDATE_DSNARRAY)
        )
    );
    /**
     * Parses a validates a config INI file
     * @param string $config_filename
     * @return \stdClass
     * @throws ErrorException
     */
    static function loadConfig($config_filename) {
        $config = parse_ini_file($config_filename, true, INI_SCANNER_NORMAL);
        $config = json_decode(json_encode($config));

        if ($config == null) {
            throw new \ErrorException('Invalid Config: Cannot parse');
        }



        $v = array();
        parse_str($config->master->DSN, $v);
        $config->master->DSN = json_decode(json_encode($v));
        ;
        if (!isset($config->master) || !isset($config->master->DSN)) {
            throw new \ErrorException('Invalid Config: Missing Master section');
        }
        $config->master->DSN->run_mode = MASTER_MODE;

        if (!isset($config->slaves) || empty($config->slaves->DSN)) {
            throw new \ErrorException('Invalid Config: Missing Slave section');
        }
        $list = array();
        foreach ($config->slaves->DSN as $str) {
            $v = array();
            parse_str($str, $v);
            if (empty($v)) {
                throw new \ErrorException('Invalid Config: Missing Slave DSN: ' . $str);
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
            'email_report' => FILTER_VALIDATE_EMAIL,
            'incremental_batchsize'=>FILTER_VALIDATE_INT,
            'expire_days'=>FILTER_VALIDATE_INT
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
            throw new \ErrorException("You can't combine options force_reset & incremental_check. Use one or another");
        }
        return $config;
    }
}
