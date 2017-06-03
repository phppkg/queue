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
     * @var \SplQueue[]
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
        foreach ($this->getPriorities() as $level) {
            $this->queues[$level] = new \SplQueue();
            $this->queues[$level]->setIteratorMode(\SplQueue::IT_MODE_DELETE);
        }
    }

    /**
     * {@inheritDoc}
     */
    protected function doPush($data, $priority = self::PRIORITY_NORM)
    {
        if (isset($this->queues[$priority])) {
            $this->queues[$priority]->enqueue($this->encode($data)); // can use push().
        }

        return true;
    }

    /**
     * {@inheritDoc}
     */
    protected function doPop()
    {
        $data = null;

        foreach ($this->queues as $queue) {
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
     * @return \SplQueue[]
     */
    public function getQueues(): array
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
    protected function close()
    {
        parent::close();

        foreach ($this->getPriorities() as $p) {
            $this->queues[$p] = null;
        }
    }
}
