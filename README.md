# reactphp-mysql-pool-server

## server pool

```
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
```


## connect pool

```
require "vendor/autoload.php";

use Wpjscc\MySQL\Pool;
use Wpjscc\MySQLServer\Factory;
use React\MySQL\QueryResult;
use React\EventLoop\Loop;

$pool = new Pool(
    '127.0.0.1:3308',
    [
        'min_connections' => 2, // 10 connection
        'max_connections' => 10, // 10 connection
        'max_wait_queue' => 70, // how many sql in queue
        'wait_timeout' => 5,// wait time include response time
        'keep_alive' => 10
    ],
    new Factory()
);

query($pool);
queryStream($pool);

function query($pool) {
    for ($i=0; $i < 90; $i++) { 
        $pool->query('select * from blog')->then(function (QueryResult $command) use ($i) {
            echo "query:$i\n";
            if (isset($command->resultRows)) {
                // this is a response to a SELECT etc. with some rows (0+)
                // print_r($command->resultFields);
                // print_r($command->resultRows);
                echo count($command->resultRows) . ' row(s) in set' . PHP_EOL;
            } else {
                // this is an OK message in response to an UPDATE etc.
                if ($command->insertId !== 0) {
                    var_dump('last insert ID', $command->insertId);
                }
                echo 'Query OK, ' . $command->affectedRows . ' row(s) affected' . PHP_EOL;
            }
        }, function ($error) {
            // the query was not executed successfully
            echo 'Error: ' . $error->getMessage() . PHP_EOL;
        });
        
    }
}


function queryStream($pool){
    for ($i=0; $i < 90; $i++) { 
        (function($pool,$i){
            $stream = $pool->queryStream('select * from blog');
           
            $stream->on('data', function ($data) use ($i) {
                // echo "queryStream:$i\n";
                // print_r($data);
            });
            $stream->on('error', function ($err) {
                echo 'Error: ' . $err->getMessage() . PHP_EOL;
            });
            $stream->on('end', function () use ($i) {
                echo 'Completed.'.$i . PHP_EOL;
            });
            
           
        })($pool, $i);
        
    }
}

$pool->translation(function ($connection) {
    React\Async\await($connection->query("INSERT INTO blog_test (content) VALUES ('hello world success')"));
    return \React\Promise\resolve('hello world');
})->then(function($result){
    var_dump($result);
}, function($error){
    var_dump($error->getMessage());
});


```

