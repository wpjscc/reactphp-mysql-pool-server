<?php

namespace Wpjscc\MySQLServer;

use React\MySQL\QueryResult;

class Translation
{
    protected $queues = [];

    protected $connection;

    protected $mysqlConnection;

    protected $deferred;

    protected $consuming = false;

    public function __construct()
    {
        $this->deferred = new \React\Promise\Deferred();
    }


    // server connection
    public function setConnection($connection)
    {
        $this->connection = $connection;
    }

    // mysql 连接
    public function setMysqlConnection($mysqlConnection)
    {
        $this->mysqlConnection = $mysqlConnection;
    }

    public function addQueue(\Psr\Http\Message\ResponseInterface $response)
    {
        $this->queues[] = $response;   
    }

    public function statrtConsume()
    {

        if ($this->consuming) {
            return;
        }

        $this->consuming = true;

        $consume = function () use (&$consume) {
            if (count($this->queues) > 0) {
                \React\Async\async(function ($resposne) use ($consume)  {
                    // 一个个消费队列中的 sql
                    \React\Async\await($this->handleResponse($resposne));
                    $consume();
                })(array_shift($this->queues));
            } else {
                $this->consuming = false;
            }
        };

        $consume();

        return $this->deferred->promise();

    }

    public function handleResponse(\Psr\Http\Message\ResponseInterface $response)
    {
        // 收到 请求了
        // query
        if ($response->getStatusCode() === 201) {
            return $this->handle201($response);
        }
        // query stream
        elseif ($response->getStatusCode() === 202) {
            return $this->handle202($response);
        } 

    }

    public function handle201(\Psr\Http\Message\ResponseInterface $response)
    {

        $connection = $this->connection;

        $body = $response->getHeaderLine('Body');
        $body = base64_decode($body);
        $query = json_decode($body, true);
        $sql = $query['sql'];
        $binds = $query['binds'] ?? [];

        return $this->mysqlConnection->query($sql, $binds)->then(function (QueryResult $command) use ($connection, $sql, $response) {
            $connection->write(implode("\r\n", [
                'HTTP/1.1 201 Query',
                'spl_object_hash: ' . $response->getHeaderLine('spl_object_hash'),
                'Body: ' . base64_encode(json_encode($command)),
                "\r\n"
            ]));
            if ($sql ==='COMMIT' || $sql === 'ROLLBACK') {
                $this->deferred->resolve(true);
            }
            return true;
        }, function ($error){
            $this->deferred->reject($error);
            return $error;
        });

    }

    public function handle202(\Psr\Http\Message\ResponseInterface $response)
    {

        $defer = new \React\Promise\Deferred();
        
        $connection = $this->connection;
       
        $body = $response->getHeaderLine('Body');
        $body = base64_decode($body);
        $query = json_decode($body, true);
        $sql = $query['sql'];
        $binds = $query['binds'] ?? [];
        $stream = $this->mysqlConnection->queryStream($sql, $binds);

        $stream->on('data', function ($data) use ($connection, $response)  {
            $connection->write(implode("\r\n", [
                'HTTP/1.1 202 Stream',
                'spl_object_hash: ' . $response->getHeaderLine('spl_object_hash'),
                'Body: ' . base64_encode(json_encode($data)),
                "\r\n"
            ]));
        });

        $stream->on('error', function ($error) use ($defer) {
            $this->deferred->reject($error);
            $defer->reject($error);
        });

        $stream->on('end', function () use ($connection, $defer, $response)  {
            $connection->write(implode("\r\n", [
                'HTTP/1.1 203 Stream End',
                'spl_object_hash: ' . $response->getHeaderLine('spl_object_hash'),
                "\r\n"
            ]));
            $defer->resolve(true);
        });

        return $defer->promise();

    }


    public function getDeferred()
    {
        return $this->deferred;
    }

}
