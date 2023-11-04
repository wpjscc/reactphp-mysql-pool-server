<?php

namespace Wpjscc\MySQLServer;

use React\MySQL\Factory as MySQLFactory;
use React\Socket\ConnectionInterface as SocketConnectionInterface;
use React\EventLoop\Loop;
use React\EventLoop\LoopInterface;
use React\Socket\ConnectorInterface;
use React\Socket\Connector;
use React\MySQL\Io\LazyConnection;

class Factory extends MySQLFactory
{
    /** @var LoopInterface */
    private $loop;

    /** @var ConnectorInterface */
    private $connector;

    public function __construct(LoopInterface $loop = null, ConnectorInterface $connector = null)
    {
        $this->loop = $loop ?: Loop::get();
        $this->connector = $connector ?: new Connector([], $this->loop);
    }

    public function createConnection(
        $uri
    ) {
        return $this->connector->connect($uri)->then(function(SocketConnectionInterface $connection){
            return new Connection($connection);
        });
    }

    public function createLazyConnection(
        #[\SensitiveParameter]
        $uri
    ) {
        return new LazyConnection($this, $uri, $this->loop);
    }
}