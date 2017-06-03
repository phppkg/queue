<?php
/**
 * Created by PhpStorm.
 * User: inhere
 * Date: 2017/5/31
 * Time: 下午8:08
 */

namespace inhere\queue;

/**
 * Class ShmQueue - shared memory queue
 * @package inhere\queue
 */
class ShmQueue extends BaseQueue
{
    /**
     * @var array
     */
    private $queues = [];

    /**
     * @var array
     */
    protected $config = [
        'id' => null,
        'serialize' => true,

        'size' => 256000,
        'project' => 'php_shm', // shared memory, semaphore
        'tmpPath' => './', // tmp path
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
            $this->id = $this->config['id'] = $this->ftok(__FILE__, $this->config['project']);
        }

        // create queues
        foreach ($this->getIntChannels() as $id) {
            $this->queues[] = null; // isset() will return false.
        }
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
     * @return mixed
     */
    protected function doPop()
    {
        // TODO: Implement doPop() method.
    }

    /**
     * {@inheritDoc}
     */
    protected function close()
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

    protected function write($shmId, $name, $value)
    {

    }

    protected function read($shmId, $name = null)
    {
        $key = $this->getIntChannels()[$priority];
    }

    protected function delete($shmId, $name)
    {
        $map = $this->read($shmId);
    }

    /**
     * 共享锁定
     * @param int|string $semId
     * @return resource
     */
    private function lock($semId)
    {
        if (function_exists('sem_get')) {
            $fp = sem_get($semId);
            sem_acquire($fp);
        } else {
            $fp = fopen($this->config['tmpPath'] . '/' . md5($semId) . '.sem', 'w');
            flock($fp, LOCK_EX);
        }

        return $fp;
    }

    /**
     * 解除共享锁定
     * @param resource $fp
     * @return bool
     */
    private function unlock(&$fp)
    {
        if (function_exists('sem_release')) {
            return sem_release($fp);
        } else {
            return fclose($fp);
        }
    }

    /**
     * @param $pathname
     * @param $projectId
     * @return int|string
     */
    private function ftok($pathname, $projectId)
    {
        if (function_exists('ftok')) {
            return ftok($pathname, $projectId);
        }

        if (!$st = @stat($pathname)) {
            return -1;
        }

        $key = sprintf("%u", (($st['ino'] & 0xffff) | (($st['dev'] & 0xff) << 16) | (($projectId & 0xff) << 24)));

        return $key;
    }
}
