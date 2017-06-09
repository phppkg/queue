<?php
/**
 * Created by PhpStorm.
 * User: inhere
 * Date: 2017/6/10
 * Time: 上午12:32
 */

namespace inhere\queue\utils;

/**
 * Class PushFailedLoader
 * @package inhere\queue\utils
 */
class PushFailedLoader
{
    /**
     * PushFailedLoader constructor.
     * @param string $file failed data save file
     */
    public function __construct($file)
    {
        $this->file = new \SplFileObject($file);
    }
}
