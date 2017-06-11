<?php
/**
 * Created by PhpStorm.
 * User: inhere
 * Date: 2017/6/10
 * Time: 上午12:18
 */

namespace inhere\queue\utils;

use inhere\queue\QueueInterface;

/**
 * Class PushFailHandler
 * @package inhere\queue\utils
 */
class PushFailHandler
{
    /**
     * @var string
     */
    public $savePath;

    public $fileExt = '.data';

    /**
     * @param $data
     * @param $priority
     * @param QueueInterface $queue
     */
    public function __invoke($data, $priority, QueueInterface $queue)
    {
        file_put_contents($this->getFile(), json_encode([
            'time' => time(),
            'priority' => $priority,
            'driver' => $queue->getDriver(),
            'id' => $queue->getId(),
            'intId' => $queue->getChannels()[$priority],
            'strId' => $queue->getIntChannels()[$priority],
            'data'  => $data,
        ]));
    }

    /**
     * @return string
     */
    public function getFile()
    {
        if (!$this->savePath) {
            $this->savePath = dirname(__DIR__);
        }

        return $this->savePath . DIRECTORY_SEPARATOR . 'failed_' . date('Ymd') . $this->fileExt;
    }
}
