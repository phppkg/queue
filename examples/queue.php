<?php
/**
 * Created by PhpStorm.
 * User: inhere
 * Date: 2017/5/29
 * Time: 上午11:05
 */


use inhere\queue\QueueInterface;

require dirname(__DIR__) . '/../../autoload.php';

$q = \inhere\queue\QueueFactory::make([
    'driver' => 'sysv',
    'id' => 12,
]);
//var_dump($q);

$q->push('n1');
$q->push('n2');
$q->push(['array-value']);
$q->push('h1', QueueInterface::PRIORITY_HIGH);
$q->push('l1', QueueInterface::PRIORITY_LOW);

$i = 5;

while ($i--) {
    var_dump($q->pop());
    usleep(50000);
}
