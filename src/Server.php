<?php

namespace Wpjscc\MySQLServer;

use React\MySQL\QueryResult;
use React\MySQL\ConnectionInterface;

class Server
{
    protected $pool;

    protected $translations;

    protected $closeingConnections;

    public function __construct($port, $pool)
    {
        $this->pool = $pool;
        $this->translations = new \SplObjectStorage;
        $this->closeingConnections = new \SplObjectStorage;
        $socket = new \React\Socket\SocketServer("0.0.0.0:$port");
        $socket->on('connection', function ($connection) {
            $parse = new ParseBuffer();
            $parse->setConnection($connection);
            $connection->on('data', [$parse, 'handleBuffer']);
            $parse->on('response', [$this, 'handleResponse']);
            $connection->on('close', function () use ($connection) {
                if ($this->translations->contains($connection)) {
                    $this->translations[$connection]['translation']->getDeferred()->reject(new \Exception('connection closed'));
                }
                if ($this->closeingConnections->contains($connection)) {
                    $this->closeingConnections->detach($connection);
                }
            });
        });

    }


    public function handleResponse(\Psr\Http\Message\ResponseInterface $response, $parse) 
    {
        if (!$this->translations->contains($parse->getConnection()) && $this->closeingConnections->contains($parse->getConnection())) {
            $parse->getConnection()->write(implode("\r\n", [
                'HTTP/1.1 500 Closing',
                "spl_object_hash: ". $response->getHeaderLine('spl_object_hash'),
                'Body: ' . base64_encode('connection is closing'),
                "\r\n"
            ]));
            return;
        }

        // query
        if ($response->getStatusCode() === 201) {
            $this->handle201($response, $parse);
        }
        // query stream
        elseif ($response->getStatusCode() === 202) {
            $this->handle202($response, $parse);
        } 
        // client ping       
        elseif ($response->getStatusCode() === 300) {
            $parse->getConnection()->write("HTTP/1.1 301 Pong\r\nspl_object_hash: {$response->getHeaderLine('spl_object_hash')}\r\n\r\n");
        }
        // client request close
        elseif ($response->getStatusCode() === 302) {
            $parse->getConnection()->write("HTTP/1.1 302 Closing\r\nspl_object_hash: {$response->getHeaderLine('spl_object_hash')}\r\n\r\n");
            $this->closeingConnections->attach($parse->getConnection());
        }
    }

    // query
    public function handle201(\Psr\Http\Message\ResponseInterface $response, $parse)
    {

        $connection = $parse->getConnection();

        if ($this->translations->contains($connection)) {
            $translation = $this->translations[$connection]['translation'];
            $translation->addQueue($response);
            $translation->statrtConsume();
            return;
        }

        $body = $response->getHeaderLine('Body');
        $body = base64_decode($body);
        $query = json_decode($body, true);
        $sql = $query['sql'];
        $binds = $query['binds'] ?? [];

        if ($sql === 'BEGIN') {
            $this->handle303($response, $parse);
        } else {
            $this->pool->query($sql, $binds)->then(function (QueryResult $command) use ($connection, $response) {
                $connection->write(implode("\r\n", [
                    'HTTP/1.1 201 Query',
                    "spl_object_hash: ". $response->getHeaderLine('spl_object_hash'),
                    'Body: ' . base64_encode(json_encode($command)),
                    "\r\n"
                ]));
            }, function ($error) use ($connection, $response) {
                $connection->write(implode("\r\n", [
                    'HTTP/1.1 500 Error',
                    "spl_object_hash: ". $response->getHeaderLine('spl_object_hash'),
                    'Body: ' . base64_encode($error->getMessage()),
                    "\r\n"
                ]));
            });
        }

       

    }

    // query stream

    public function handle202(\Psr\Http\Message\ResponseInterface $response, $parse)
    {

        $connection = $parse->getConnection();

        if ($this->translations->contains($connection)) {
            $translation = $this->translations[$connection]['translation'];
            $translation->addQueue($response);
            $translation->statrtConsume();
            return;
        }

        $body = $response->getHeaderLine('Body');
        $body = base64_decode($body);
        $query = json_decode($body, true);
        $sql = $query['sql'];
        $binds = $query['binds'] ?? [];

        $stream = $this->pool->queryStream($sql, $binds);

        $stream->on('data', function ($data) use ($connection, $response)  {
            $connection->write(implode("\r\n", [
                'HTTP/1.1 202 Stream',
                "spl_object_hash: ". $response->getHeaderLine('spl_object_hash'),
                'Body: ' . base64_encode(json_encode($data)),
                "\r\n"
            ]));
        });

        $stream->on('error', function ($error) use ($connection, $response) {
            $connection->write(implode("\r\n", [
                'HTTP/1.1 500 Stream Error',
                "spl_object_hash: ". $response->getHeaderLine('spl_object_hash'),
                'Body: ' . base64_encode($error->getMessage()),
                "\r\n"
            ]));
        });

        $stream->on('end', function () use ($connection, $response)  {
            $connection->write(implode("\r\n", [
                'HTTP/1.1 203 Stream End',
                "spl_object_hash: ". $response->getHeaderLine('spl_object_hash'),
                "\r\n"
            ]));
        });

    }

    // start translation
    public function handle303(\Psr\Http\Message\ResponseInterface $response, $parse)
    {
        $connection = $parse->getConnection();

        if ($this->translations->contains($connection)) {
            // todo handle error
            return;
        }
        $translation = new Translation();
        $translation->setConnection($connection);
        $translation->addQueue($response);
        $this->translations->attach($connection, [
            'translation' => $translation,
        ]);

        $translation->getDeferred()->promise()->then(null, function ($error) use ($connection, $response) {
            $connection->write(implode("\r\n", [
                'HTTP/1.1 500 Error',
                "spl_object_hash: ". $response->getHeaderLine('spl_object_hash'),
                'Body: ' . base64_encode($error->getMessage()),
                "\r\n"
            ]));
            $this->translations->detach($connection);
            return $error;
        });

        $this->pool->getIdleConnection()->then(function (ConnectionInterface $mysqlConnection) use ($connection, $translation)  {
            $translation->setMysqlConnection($mysqlConnection);
            $translation->statrtConsume()->then(function () use ($connection, $mysqlConnection) {
                $this->translations->detach($connection);
                $this->pool->releaseConnection($mysqlConnection);
            }, function($error) use ($mysqlConnection) {
                $this->pool->releaseConnection($mysqlConnection);
                return $error;
            });
        }, function ($error) use ($translation)  {
            $translation->getDeferred()->reject($error);
            return $error;
        });
       
    }
}