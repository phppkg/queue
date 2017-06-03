<?php
/**
 * Created by PhpStorm.
 * User: inhere
 * Date: 2017/5/21
 * Time: 上午1:45
 */

namespace inhere\queue;

/**
 * Class SSDBQueue
 * @package inhere\queue
 * @link http://ssdb.io
 */
class SSDBQueue extends BaseQueue
{
    /**
     * @var string
     */
    protected $driver = QueueFactory::DRIVER_SSDB;

    /**
     * ssdb
     * @var \SimpleSSDB
     */
    private $ssdb;

    /**
     * SSDBQueue constructor.
     * @param array $config
     */
    public function __construct(array $config = [])
    {
        if (isset($config['ssdb'])) {
            $this->setSSDB($config['ssdb']);
            unset($config['ssdb']);
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

            return $this->ssdb->qPush($channels[$priority], $data);
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
            if ($data = $this->ssdb->qPop($channel)) {
                $data = $this->decode($data);
                break;
            }
        }

        return $data;
    }

    /**
     * @return \SSDB
     */
    public function getSSDB(): \SSDB
    {
        return $this->ssdb;
    }

    /**
     * @param \SSDB $ssdb
     */
    public function setSSDB(\SSDB $ssdb)
    {
        $this->ssdb = $ssdb;
    }
}
