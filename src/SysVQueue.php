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
     * @var \SplFixedArray
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

        // 初始化队列列表. 使用时再初始化需要的队列
        $this->queues = new \SplFixedArray(count($this->getPriorities()));
    }

    /**
     * {@inheritdoc}
     */
    protected function doPush($data, $priority = self::PRIORITY_NORM)
    {
        // $blocking = true 如果队列满了，这里会阻塞
        // bool msg_send(
        //      resource $queue, int $msgtype, mixed $message
        //      [, bool $serialize = true [, bool $blocking = true [, int &$errorcode ]]]
        // )

        if (!$this->isPriority($priority)) {
            $priority = self::PRIORITY_NORM;
        }

        // create queue if it not exists.
        $this->createQueue($priority);

        return msg_send(
            $this->queues[$priority],
            $this->msgType,
            $this->encode($data),
            false,
            $this->config['blocking'],
            $this->errCode
        );
    }

    /**
     * {@inheritDoc}
     */
    protected function doPop($priority = null, $block = false)
    {
        // bool msg_receive(
        //      resource $queue, int $desiredmsgtype, int &$msgtype, int $maxsize,
        //      mixed &$message [, bool $unserialize = true [, int $flags = 0 [, int &$errorcode ]]]
        //  )

        // 只想取出一个 $priority 队列的数据
        if ($this->isPriority($priority)) {
            // $priority 级别的队列还未初始化。create queue if it not exists.
            $this->createQueue($priority);
            $flags = $block ? 0 : (MSG_IPC_NOWAIT | MSG_NOERROR);

            $success = msg_receive(
                $this->queues[$priority],
                0,  // 0 $this->msgType,
                $this->msgType,   // $this->msgType,
                $this->config['bufferSize'],
                $data,
                false,

                // 0: 默认值，无消息后会阻塞等待。(要取多个队列数据时，不能用它，不然无法读取后面两个队列的数据)
                // MSG_IPC_NOWAIT 无消息后不等待
                // MSG_EXCEPT
                // MSG_NOERROR 消息超过大小限制时，截断数据而不报错
                $flags,
                $this->errCode
            );

            if ($success) {
                return $this->decode($data);
            }

            return null;
        }

        $data = null;

        foreach ($this->queues as $priority => $queue) {
            if (($data = $this->doPop($priority, false)) !== null) {
                $data = $this->decode($data);
                break;
            }
        }

        return $data;
    }

    /**
     * @param int $priority
     */
    protected function createQueue($priority)
    {
        if (!$this->queues[$priority]) {
            $key = $this->getIntChannels()[$priority];
            $this->queues[$priority] = msg_get_queue($key);
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
    public function close()
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
