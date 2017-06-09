<?php
/**
 * Created by PhpStorm.
 * User: inhere
 * Date: 2017/5/21
 * Time: 上午1:45
 */

namespace inhere\queue;

/**
 * Class PhpQueue
 * @package inhere\queue
 */
class PhpQueue extends BaseQueue
{
    /**
     * @var string
     */
    protected $driver = QueueFactory::DRIVER_PHP;

    /**
     * @var \SplFixedArray
     */
    private $queues = [];

    /**
     * {@inheritdoc}
     */
    protected function init()
    {
        parent::init();

        if (!$this->id) {
            $this->id = $this->driver;
        }

        // create queues
        $this->queues = new \SplFixedArray(count($this->getPriorities()));
    }

    /**
     * {@inheritDoc}
     */
    protected function doPush($data, $priority = self::PRIORITY_NORM)
    {
        if (!$this->isPriority($priority)) {
            $priority = self::PRIORITY_NORM;
        }

        $this->createQueue($priority);

        return $this->queues[$priority]->enqueue($this->encode($data)); // can use push().
    }

    /**
     * {@inheritDoc}
     */
    protected function doPop($priority = null, $block = false)
    {
        // 只想取出一个 $priority 队列的数据
        if ($this->isPriority($priority)) {
            $this->createQueue($priority);
            $data = $this->queues[$priority]->dequeue();

            return $this->decode($data);
        }

        $data = null;

        foreach ($this->queues as $queue) {
            $this->createQueue($priority);

            // valid()
            if (!$queue->isEmpty()) {
                // can use shift().
                $data = $this->decode($queue->dequeue());
                break;
            }
        }

        // reset($this->queues);
        return $data;
    }

    /**
     * @param int $priority
     */
    protected function createQueue($priority)
    {
        if (!$this->queues[$priority]) {
            $this->queues[$priority] = new \SplQueue();
            $this->queues[$priority]->setIteratorMode(\SplQueue::IT_MODE_DELETE);
        }
    }

    /**
     * @return \SplFixedArray
     */
    public function getQueues()
    {
        return $this->queues;
    }

    /**
     * @param int $priority
     * @return \SplQueue|false
     */
    public function getQueue($priority = self::PRIORITY_NORM)
    {
        if (!isset($this->getPriorities()[$priority])) {
            return false;
        }

        return $this->queues[$priority];
    }

    /**
     * @param int $priority
     * @return array|null
     */
    public function getStat($priority = self::PRIORITY_NORM)
    {
        if ($q = $this->getQueue($priority)) {
            return [
                'num' => $q->count(),
            ];
        }

        return null;
    }

    /**
     * close
     */
    public function close()
    {
        parent::close();

        foreach ($this->getPriorities() as $p) {
            $this->queues[$p] = null;
        }
    }
}
