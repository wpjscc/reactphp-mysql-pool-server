<?php

namespace Wpjscc\MySQLServer;

use React\MySQL\ConnectionInterface;
use React\Socket\ConnectionInterface as SocketConnectionInterface;
use React\Promise\Deferred;
use React\Stream\ThroughStream;
use React\MySQL\QueryResult;
use React\Promise\Timer\TimeoutException;

class Connection implements ConnectionInterface
{
    use \Evenement\EventEmitterTrait;

    const STATE_AUTHENTICATED       = 5;

    const STATE_CLOSEING            = 6;
    const STATE_CLOSED              = 7;


    /**
     * @var integer
     */
    private $state = self::STATE_AUTHENTICATED;

    protected $stream;

    protected $deferredOrStreams = [];

    protected $closeDeferred;

    protected $hashs = [];

    public function __construct(SocketConnectionInterface $connection)
    {
        $this->stream = $connection;
        $parserBuffer = new ParseBuffer();
        $this->stream->on('data', [$parserBuffer, 'handleBuffer']);
        $parserBuffer->on('response', [$this, 'handleResposne']);

        $this->stream->on('error', [$this, 'handleConnectionError']);
        $this->stream->on('close', [$this, 'handleConnectionClosed']);
    }


    public function query($sql, array $params = [])
    {

        if ($this->state === self::STATE_CLOSEING) {
            return \React\Promise\reject(new \Exception('Connection is Closing'));
        }

        if ($this->state === self::STATE_CLOSED) {
            return \React\Promise\reject(new \Exception('Connection had closed'));
        }

        $deferred = new Deferred();

        $hash = spl_object_hash($deferred);
        $this->deferredOrStreams[$hash] = $deferred;

        $this->stream->write(implode("\r\n", [
            "HTTP/1.1 201 OK",
            "spl_object_hash: " . $hash,
            "Body: " . base64_encode(json_encode([
                'sql' => $sql,
                'binds' => $params,
            ])),
            "\r\n"
        ]));

        return $this->timeout($deferred->promise(), 3)->then(null, function ($e) use ($hash) {
            $this->clearHash($hash);
            throw $e;
        });
    }

    public function queryStream($sql, $params = [])
    {
        if ($this->state === self::STATE_CLOSEING) {
            return \React\Promise\reject(new \Exception('Connection is Closing'));
        }

        if ($this->state === self::STATE_CLOSED) {
            return \React\Promise\reject(new \Exception('Connection had closed'));
        }

        $stream = new ThroughStream;
        $hash = spl_object_hash($stream);
        $this->deferredOrStreams[$hash] = $stream;
        array_push($this->hashs, $hash);
        $this->stream->write(implode("\r\n", [
            "HTTP/1.1 202 OK",
            "spl_object_hash: " . $hash,
            "Body: " . base64_encode(json_encode([
                'sql' => $sql,
                'binds' => $params,
            ])),
            "\r\n"
        ]));

        $deferred = new Deferred();

        $this->timeout($deferred->promise(), 30)->then(null, function ($e) use ($hash, $stream) {
            $this->clearHash($hash);
            $stream->emit('error', [$e]);
        });

        $stream->on('end', function () use ($deferred) {
            $deferred->resolve(null);
        });


        return $stream;
    }

    public function handleResposne($response, $parser)
    {

        $spl_object_hash = $response->getHeaderLine('spl_object_hash');
        $deferredOrStream = $this->deferredOrStreams[$spl_object_hash] ?? null;

        if (!$deferredOrStream) {
            return;
        }

        // query response
        if ($response->getStatusCode() === 201) {
            $body = $response->getHeaderLine("Body");
            $body = json_decode(base64_decode($body), true);
            $queryResult = new QueryResult();
            $queryResult->insertId = $body["insertId"] ?? null;
            $queryResult->affectedRows = $body["affectedRows"] ?? null;
            $queryResult->resultFields = $body["resultFields"] ?? null;
            $queryResult->resultRows = $body["resultRows"] ?? null;
            $queryResult->resultRows = $body["resultRows"] ?? null;
            $queryResult->warningCount = $body["warningCount"] ?? null;
            $deferredOrStream->resolve($queryResult);
            $this->clearHash($spl_object_hash);
        }
        // stream response
        else if ($response->getStatusCode() === 202) {
            $body = $response->getHeaderLine("Body");
            $body = json_decode(base64_decode($body), true);
            $deferredOrStream->emit('data', [$body]);
        }
        // stream response end
        else if ($response->getStatusCode() === 203) {
            $deferredOrStream->end();
            $this->clearHash($spl_object_hash);
        }
        // server pong
        else if ($response->getStatusCode() === 301) {
            $deferredOrStream->resolve(true);
            $this->clearHash($spl_object_hash);
        }

        // server response can close
        else if ($response->getStatusCode() === 302) {
            $deferredOrStream->resolve(true);
            $this->clearHash($spl_object_hash);
        }

        // error
        else if ($response->getStatusCode() === 500) {
            $body = $response->getHeaderLine("Body");
            $body = base64_decode($body);
            if ($deferredOrStream instanceof Deferred) {
                $deferredOrStream->reject(new \Exception($body));
            } else {
                $deferredOrStream->emit('error', [new \Exception($body)]);
            }
            $this->clearHash($spl_object_hash);
        }

        // 软关闭
        if ($this->state === self::STATE_CLOSEING && empty($this->hashs)) {
            $this->close();
            $this->closeDeferred->resolve(true);
        }
    }

    public function ping()
    {
        if ($this->state === self::STATE_CLOSEING) {
            return \React\Promise\reject(new \Exception('Connection is Closing'));
        }

        if ($this->state === self::STATE_CLOSED) {
            return \React\Promise\reject(new \Exception('Connection had closed'));
        }

        $deferred = new Deferred();
        $hash = spl_object_hash($deferred);
        $this->deferredOrStreams[$hash] = $deferred;
        array_push($this->hashs, $hash);
        $this->stream->write("HTTP/1.1 300 OK\r\nspl_object_hash: {$hash}\r\n\r\n");
        return $this->timeout($deferred->promise())->then(null, function ($e) use ($hash) {
            $this->clearHash($hash);
            return $e;
        });
    }

    protected function timeout($promise, $time = 2)
    {
        return \React\Promise\Timer\timeout($promise, $time)->then(null, function ($e) {
            if ($e instanceof TimeoutException) {
                throw new \RuntimeException(
                    'timed out after ' . $e->getTimeout() . ' seconds (ETIMEDOUT)',
                    \defined('SOCKET_ETIMEDOUT') ? \SOCKET_ETIMEDOUT : 110
                );
            }
            throw $e;
        });
    }

    protected function clearHash($hash)
    {
        $deferredOrStreams = $this->deferredOrStreams[$hash] ?? null;
        unset($this->deferredOrStreams[$hash]);
        unset($this->hashs[array_search($hash, $this->hashs)]);
        return $deferredOrStreams;
    }

    public function quit()
    {

        if ($this->state === self::STATE_CLOSED) {
            return \React\Promise\resolve(true);
        }

        $this->closeDeferred = new Deferred();

        $deferred = new Deferred();
        $hash = spl_object_hash($deferred);
        $this->deferredOrStreams[$hash] = $deferred;
        array_push($this->hashs, $hash);
        $this->stream->write("HTTP/1.1 302 OK\r\nspl_object_hash: {$hash}\r\n\r\n");
        $this->state = self::STATE_CLOSEING;

        return $this->timeout($deferred->promise())->then(function () {
            return $this->timeout($this->closeDeferred->promise());
        })->then(null, function ($e) use ($hash) {
            $this->clearHash($hash);
            return $e;
        });
    }

    public function close()
    {
        if ($this->state === self::STATE_CLOSED) {
            return;
        }

        $this->state = self::STATE_CLOSED;
        $remoteClosed = $this->stream->isReadable() === false && $this->stream->isWritable() === false;
        $this->stream->close();

        while (!empty($this->hashs)) {
            $hash = array_shift($this->hashs);
            if (isset($this->deferredOrStreams[$hash])) {
                $deferredOrStream = $this->deferredOrStreams[$hash];
                unset($this->deferredOrStreams[$hash]);
            } else {
                continue;
            }
            if ($deferredOrStream instanceof Deferred) {
                if ($remoteClosed) {
                    $deferredOrStream->reject(new \RuntimeException(
                        'Connection closed by peer (ECONNRESET)',
                        \defined('SOCKET_ECONNRESET') ? \SOCKET_ECONNRESET : 104
                    ));
                } else {
                    $deferredOrStream->reject(new \RuntimeException(
                        'Connection closing (ECONNABORTED)',
                        \defined('SOCKET_ECONNABORTED') ? \SOCKET_ECONNABORTED : 103
                    ));
                }
            } else {
                if ($remoteClosed) {
                    $deferredOrStream->emit('error', [new \RuntimeException(
                        'Connection closed by peer (ECONNRESET)',
                        \defined('SOCKET_ECONNRESET') ? \SOCKET_ECONNRESET : 104
                    )]);
                } else {
                    $deferredOrStream->emit('error', [new \RuntimeException(
                        'Connection closing (ECONNABORTED)',
                        \defined('SOCKET_ECONNABORTED') ? \SOCKET_ECONNABORTED : 103
                    )]);
                }
            }
        }

        $this->emit('close');
        $this->removeAllListeners();
    }

    /**
     * @param Exception $err Error from socket.
     *
     * @return void
     * @internal
     */
    public function handleConnectionError($err)
    {
        $this->emit('error', [$err, $this]);
    }

    /**
     * @return void
     * @internal
     */
    public function handleConnectionClosed()
    {
        if ($this->state < self::STATE_CLOSEING) {
            $this->emit('error', [new \RuntimeException(
                'Connection closed by peer (ECONNRESET)',
                \defined('SOCKET_ECONNRESET') ? \SOCKET_ECONNRESET : 104
            )]);
        }

        $this->close();
    }
}
