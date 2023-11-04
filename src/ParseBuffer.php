<?php

namespace Wpjscc\MySQLServer;


use RingCentral\Psr7;
use Evenement\EventEmitter;

class ParseBuffer extends EventEmitter 
{

    protected $buffer = '';
    protected $connection;

    public function handleBuffer($buffer)
    {
        if ($buffer === '') {
            return;
        }
        
        $this->buffer .= $buffer;
        $this->parseBuffer();
    }

    protected function parseBuffer()
    {
        $pos = strpos($this->buffer, "\r\n\r\n");
        if ($pos !== false) {
            $httpPos = strpos($this->buffer, "HTTP/1.1");
            if ($httpPos === false) {
                $httpPos = 0;
            }
            try {
                $response = Psr7\parse_response(substr($this->buffer, $httpPos, $pos - $httpPos));
            } catch (\Exception $e) {

                $this->buffer = substr($this->buffer, $pos + 4);

                return;
            }

            $this->buffer = substr($this->buffer, $pos + 4);

            $this->emit('response', [$response, $this]);

            $this->parseBuffer();
        }
    }

    public function getBuffer()
    {
        return $this->buffer;
    }

    public function pullBuffer()
    {
        $buffer = $this->buffer;

        $this->buffer = '';

        return $buffer;
    }


    public function setConnection($connection)
    {
        $this->connection = $connection;
    }

    public function getConnection()
    {
        return $this->connection;
    }
   
}