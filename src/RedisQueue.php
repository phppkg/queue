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
    protected $driver = QueueFactory::DRIVER_REDIS;

    /**
     * redis
     * @var \Redis
     */
    private $redis;

    /**
     * RedisQueue constructor.
     * @param array $config
     */
    public function __construct(array $config = [])
    {
        if (isset($config['redis'])) {
            $this->setRedis($config['redis']);
            unset($config['redis']);
        }

        parent::__construct($config);

        if (!$this->id) {
            $this->id = $this->driver;
        }
    }

    /**
     * {@inheritDoc}
     */
    protected function doPush($data, $priority = self::PRIORITY_NORM)
    {
        $channels = array_values($this->getChannels());

        if (isset($channels[$priority])) {
            $data = $this->encode($data);

            return $this->redis->lPush($channels[$priority], $data);
        }

        return false;
    }

    /**
     * {@inheritDoc}
     */
    protected function doPop()
    {
        $data = null;

        foreach ($this->getChannels() as $channel) {
            if ($data = $this->redis->rPop($channel)) {
                $data = $this->decode($data);
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
