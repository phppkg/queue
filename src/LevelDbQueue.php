<?php
/**
 * Created by PhpStorm.
 * User: inhere
 * Date: 2017/5/21
 * Time: 上午1:45
 */

namespace inhere\queue;

/**
 * Class LevelDbQueue
 * @package inhere\queue
 * @link https://github.com/google/leveldb leveldb
 * @link https://github.com/reeze/php-leveldb php ext
 */
class LevelDbQueue extends BaseQueue
{
    /**
     * @var string
     */
    protected $driver = QueueFactory::DRIVER_LDB;

    /**
     * levelDb
     * @var \LevelDb
     */
    private $ldb;

    /**
     * LevelDbQueue constructor.
     * @param array $config
     */
    public function __construct(array $config = [])
    {
        if (isset($config['ldb'])) {
            $this->setLevelDb($config['ldb']);
            unset($config['ldb']);
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

            return $this->levelDb->qPush($channels[$priority], $data);
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
            if ($data = $this->levelDb->qPop($channel)) {
                $data = $this->decode($data);
                break;
            }
        }

        return $data;
    }

    /**
     * @return \LevelDb
     */
    public function getLevelDb(): \LevelDb
    {
        return $this->levelDb;
    }

    /**
     * @param \LevelDb $levelDb
     */
    public function setLevelDb(\LevelDb $levelDb)
    {
        $this->levelDb = $levelDb;
    }
}
