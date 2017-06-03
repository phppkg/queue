<?php
/**
 * Created by PhpStorm.
 * User: inhere
 * Date: 2017/5/21
 * Time: 上午1:45
 */

namespace inhere\queue;

/**
 * Class RocksDbQueue
 * @package inhere\queue
 * @link https://github.com/facebook/rocksdb rocksdb
 * @link https://github.com/Leon2012/php-rocksdb php ext
 */
class RocksDbQueue extends BaseQueue
{
    /**
     * @var string
     */
    protected $driver = QueueFactory::DRIVER_RDB;

    /**
     * rdb
     * @var \RocksDb
     */
    private $rdb;

    /**
     * RocksDbQueue constructor.
     * @param array $config
     */
    public function __construct(array $config = [])
    {
        if (isset($config['rdb'])) {
            $this->setRocksDb($config['rdb']);
            unset($config['rdb']);
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

            return $this->rdb->qPush($channels[$priority], $data);
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
            if ($data = $this->rdb->qPop($channel)) {
                $data = $this->decode($data);
                break;
            }
        }

        return $data;
    }

    /**
     * @return \RocksDb
     */
    public function getRocksDb(): \RocksDb
    {
        return $this->rdb;
    }

    /**
     * @param \RocksDb $rdb
     */
    public function setRocksDb(\RocksDb $rdb)
    {
        $this->rdb = $rdb;
    }
}
