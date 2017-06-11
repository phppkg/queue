<?php
/**
 * Created by PhpStorm.
 * User: inhere
 * Date: 2017/5/21
 * Time: 上午1:45
 */

namespace inhere\queue;

/**
 * Class RedisQueue
 * @package inhere\queue
 */
class RedisQueue extends BaseQueue
{
    /**
     * @var string
     */
    protected $driver = Queue::DRIVER_REDIS;

    /**
     * redis
     * @var \Redis
     */
    private $redis;

    protected function init()
    {
        if (!$this->id) {
            $this->id = $this->driver;
        }
    }

    /**
     * {@inheritDoc}
     */
    protected function doPush($data, $priority = self::PRIORITY_NORM)
    {
        if (!$this->isPriority($priority)) {
            $priority = self::PRIORITY_NORM;
        }

        return $this->redis->lPush($this->channels[$priority], $data);
    }

    /**
     * {@inheritDoc}
     */
    protected function doPop($priority = null, $block = false)
    {
        // 只想取出一个 $priority 队列的数据
        if ($this->isPriority($priority)) {
            $channel = $this->channels[$priority];

            return $this->redis->rPop($channel);
//            return $block ? $this->redis->brPop([$channel], 3) : $this->redis->rPop($channel);
        }

        $data = null;

        foreach ($this->channels as $channel) {
            if ($data = $this->redis->rPop($channel)) {
                break;
            }
        }

        return $data;
    }

    /**
     * @return \Redis
     */
    public function getRedis(): \Redis
    {
        return $this->redis;
    }

    /**
     * @param \Redis $redis
     */
    public function setRedis(\Redis $redis)
    {
        $this->redis = $redis;
    }
}
