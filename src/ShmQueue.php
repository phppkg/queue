<?php
/**
 * Created by PhpStorm.
 * User: inhere
 * Date: 2017/5/31
 * Time: 下午8:08
 */

namespace inhere\queue;
use inhere\library\helpers\PhpHelper;

/**
 * Class ShmQueue - shared memory queue
 * @package inhere\queue
 */
class ShmQueue extends BaseQueue
{
    /**
     * @var \SplFixedArray
     */
    private $queues = [];

    /**
     * @var array
     */
    protected $config = [
        'key' => null,
        'serialize' => true,

        'size' => 256000,
        'project' => 'php_shm', // shared memory, semaphore
        'tmpDir' => '/tmp', // tmp path
    ];

    /**
     * {@inheritDoc}
     */
    protected function init()
    {
        parent::init();

        if ($this->config['id'] > 0) {
            $this->id = (int)$this->config['id'];
        } else {
            // 定义共享内存,信号量key
            $this->id = $this->config['id'] = PhpHelper::ftok(__FILE__, $this->config['project']);
        }

        // create queues
        $this->queues = new \SplFixedArray(count($this->getPriorities()));
    }

    /**
     * @param $data
     * @param int $priority
     * @return bool
     */
    protected function doPush($data, $priority = self::PRIORITY_NORM)
    {
        // TODO: Implement doPush() method.
    }

    /**
     * {@inheritDoc}
     */
    protected function doPop($priority = null, $block = false)
    {
        // TODO: Implement doPop() method.
    }

    /**
     * {@inheritDoc}
     */
    public function close()
    {
        // 释放共享内存与信号量
//        shm_remove($shareMemory);
//        sem_remove($semaphore);
    }

    /**
     * init the shared memory block
     * @param int $priority
     */
    protected function initShmBlock($priority = self::PRIORITY_NORM)
    {
        /**
         * resource shmop_open ( int $key , string $flags , int $mode , int $size )
         * $flags:
         *      a 访问只读内存段
         *      c 创建一个新内存段，或者如果该内存段已存在，尝试打开它进行读写
         *      w 可读写的内存段
         *      n 创建一个新内存段，如果该内存段已存在，则会失败
         * $mode: 八进制格式  0655
         * $size: 开辟的数据大小 字节
         */

        if (!$this->queues[$priority]) {
            $key = $this->getIntChannels()[$priority];
            $this->queues[$priority] = shmop_open($key, 'c', 0644, $this->config['size']);
        }
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
     * @return \SplQueue|false
     */
    public function getQueue($priority = self::PRIORITY_NORM)
    {
        if (!isset($this->getPriorities()[$priority])) {
            return false;
        }

        return $this->queues[$priority];
    }

}
