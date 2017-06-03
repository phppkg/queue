<?php
/**
 * @from https://github.com/matyhtf/framework/blob/master/libs/Swoole/Queue/MsgQ.php
 */

namespace inhere\queue;

/**
 * Class SysVQueue - by system v message queue
 * @package inhere\queue
 */
class SysVQueue extends BaseQueue
{
    /**
     * @var string
     */
    protected $driver = QueueFactory::DRIVER_SYSV;

    /**
     * @var int
     */
    private $msgType = 1;

    /**
     * @var resource[]
     */
    private $queues = [];

    /**
     * @var array
     */
    protected $config = [
        'id' => 0, // int
        'uniKey' => 0, // int|string
        'msgType' => 1,
        'blocking' => 1, // 0|1
        'serialize' => true, // if set False, cannot direct save array|object
        'bufferSize' => 2048, // 8192 65525
        'removeOnClose' => true, // Whether remove message queue on close.
    ];

    /**
     * {@inheritDoc}
     */
    protected function init()
    {
        // php --rf msg_send
        if (!function_exists('msg_receive')) {
            throw new \RuntimeException(
                'To enable System V semaphore,shared-memory,messages support compile PHP with the option --enable-sysvsem --enable-sysvshm --enable-sysvmsg.',
                -500
            );
        }

        parent::init();

        $this->config['id'] = (int)$this->config['id'];
        $this->config['bufferSize'] = (int)$this->config['bufferSize'];
        $this->config['blocking'] = (bool)$this->config['blocking'];
        $this->config['removeOnClose'] = (bool)$this->config['removeOnClose'];

        if ($this->config['id'] > 0) {
            $this->id = $this->config['id'];
        } else {
            $this->id = $this->config['id'] = ftok(__FILE__, $this->config['uniKey']);
        }

        $this->msgType = (int)$this->config['msgType'];

        // create queues
        foreach ($this->getIntChannels() as $id) {
            $this->queues[] = msg_get_queue($id);
        }
    }

    /**
     * {@inheritdoc}
     */
    protected function doPush($data, $priority = self::PRIORITY_NORM)
    {
        // 如果队列满了，这里会阻塞
        // bool msg_send(
        //      resource $queue, int $msgtype, mixed $message
        //      [, bool $serialize = true [, bool $blocking = true [, int &$errorcode ]]]
        // )

        if (isset($this->queues[$priority])) {
            return msg_send(
                $this->queues[$priority],
                $this->msgType,
                $this->encode($data),
                false,
                $this->config['blocking'],
                $this->errCode
            );
        }

        return false;
    }

    /**
     * {@inheritDoc}
     */
    protected function doPop()
    {
        // bool msg_receive(
        //      resource $queue, int $desiredmsgtype, int &$msgtype, int $maxsize,
        //      mixed &$message [, bool $unserialize = true [, int $flags = 0 [, int &$errorcode ]]]
        //  )

        $data = null;

        foreach ($this->queues as $queue) {
            $success = msg_receive(
                $queue,
                0,  // 0 $this->msgType,
                $this->msgType,   // $this->msgType,
                $this->config['bufferSize'],
                $data,
                false,

                // 0: 默认值，无消息后会阻塞等待。(这里不能用它，不然无法读取后面两个队列的数据)
                // MSG_IPC_NOWAIT 无消息后不等待
                // MSG_EXCEPT
                // MSG_NOERROR 消息超过大小限制时，截断数据而不报错
                MSG_IPC_NOWAIT | MSG_NOERROR,
                $this->errCode
            );

            if ($success) {
                $data = $this->decode($data);
                break;
            }
        }

        return $data;
    }

    /**
     * @return resource[]
     */
    public function getQueues(): array
    {
        return $this->queues;
    }

    /**
     * @param int $priority
     * @return resource|false
     */
    public function getQueue($priority = self::PRIORITY_NORM)
    {
        if (!isset($this->getPriorities()[$priority])) {
            return false;
        }

        return $this->queues[$priority];
    }

    /**
     * @return array
     */
    public function allQueues()
    {
        $aQueues = [];

        exec('ipcs -q', $aQueues);

        return $aQueues;
    }

    /**
     * Setting the queue option
     * @param array $options
     * @param int $queue
     */
    public function setQueueOptions(array $options = [], $queue = self::PRIORITY_NORM)
    {
        msg_set_queue($this->queues[$queue], $options);
    }

    /**
     * @param int $id
     * @return bool
     */
    public function exist($id)
    {
        return msg_queue_exists($id);
    }

    /**
     * close
     */
    protected function close()
    {
        parent::close();

        foreach ($this->queues as $key => $queue) {
            if ($queue) {
                if ($this->config['removeOnClose']) {
                    msg_remove_queue($queue);
                }

                $this->queues[$key] = null;
            }
        }
    }

    /**
     * @return array
     */
    public function getStats()
    {
        $stats = [];

        foreach ($this->queues as $queue) {
            $stats[] = msg_stat_queue($queue);
        }

        return $stats;
    }

    /**
     * @param int $queue
     * @return array
     */
    public function getStat($queue = self::PRIORITY_NORM)
    {
        return msg_stat_queue($this->queues[$queue]);
    }

}
