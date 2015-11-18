#!/usr/bin/env php
<?php
date_default_timezone_set('UTC');
error_reporting(-1);
ini_set('display_errors', 1);
require __DIR__.'/../src/bootstrap.php';
use Replication\Compiler;


try {
    $compiler = new Compiler();
    $compiler->compile();
    echo "File compiled\n";
    
} catch (\UnexpectedValueException $e) {
    echo 'Failed to compile phar: ['.get_class($e).'] '.$e->getMessage().' at '.$e->getFile().':'.$e->getLine();
    echo "\nTry executing php -d phar.readonly=false -f ".__FILE__."\n";
    exit(1);
    
} catch (\Exception $e) {
    echo 'Failed to compile phar: ['.get_class($e).'] '.$e->getMessage().' at '.$e->getFile().':'.$e->getLine();
    exit(1);
}