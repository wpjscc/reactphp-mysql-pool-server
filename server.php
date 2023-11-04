<?php

require "vendor/autoload.php";

use Wpjscc\MySQLServer\Server;
use Wpjscc\MySQL\Pool;


new Server(3308, new Pool(
    getenv('MYSQL_URI') ?: 'username:password@host/databasename?timeout=5',
    [
        'min_connections' => 2, // 10 connection
        'max_connections' => 10, // 10 connection
        'max_wait_queue' => 70, // how many sql in queue
        'wait_timeout' => 5, // wait time include response time
        'keep_alive' => 60
    ]
));
