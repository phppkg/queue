<?php
/**
 * Created by PhpStorm.
 * User: inhere
 * Date: 2017/5/21
 * Time: 上午1:26
 */

namespace inhere\queue;

/**
 * Interface QueueInterface
 * @package inhere\queue
 */
interface QueueInterface
{
    /**
     * Priorities
     */
    const PRIORITY_HIGH = 0;
    const PRIORITY_NORM = 1;
    const PRIORITY_LOW = 2;

    const PRIORITY_LOW_SUFFIX = '_low';
    const PRIORITY_HIGH_SUFFIX = '_high';

    /**
     * Events list
     */
    const EVENT_BEFORE_PUSH = 'beforePush';
    const EVENT_AFTER_PUSH = 'afterPush';

    const EVENT_BEFORE_POP = 'beforePop';
    const EVENT_AFTER_POP = 'afterPop';

    /**
     * push data
     * @param mixed $data
     * @param int $priority
     * @return bool
     */
    public function push($data, $priority = self::PRIORITY_NORM);

    /**
     * pop data
     * @param null|int $priority If is Null, pop all queue data by priority.
     * @param bool $block Whether block pop
     * @return mixed
     */
    public function pop($priority = null, $block = false);

    /**
     * @return string|int
     */
    public function getId();

    /**
     * @return string
     */
    public function getDriver();

    /**
     * @return int
     */
    public function getErrCode();

    /**
     * @return int
     */
    public function getErrMsg();
}
