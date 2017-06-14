# php 的队列实现

php的队列使用包装, 默认自带支持 `高 high` `中 norm` `低 low` 三个级别的队列操作。

- `DbQueue` 基于数据库的队列实现
- `PhpQueue` 基于 php `SplQueue` 实现
- `RedisQueue` 基于 redis 实现
- `ShmQueue` 基于共享内存实现
- `SysVQueue` 基于 *nix 系统的 system v message 实现. php 需启用 `--enable-sysvmsg` 通常是默认开启的 :) 

## 安装

- composer

```json
{
    "require": {
        "inhere/queue": "dev-master"
    }
}
```

- 直接拉取

```bash
git clone https://git.oschina.net/inhere/php-queue.git // git@osc
git clone https://github.com/inhere/php-queue.git // github
```

## 使用

```php
// file: examples/queue.php
use inhere\queue\QueueInterface;

// require __DIR__ . '/autoload.php';

$q = \inhere\queue\Queue::make([
    'driver' => 'sysv', // shm sysv php
    'id' => 12,
]);
//var_dump($q);

$q->push('n1');
$q->push('n2');
$q->push(['n3-array-value']);
$q->push('h1', QueueInterface::PRIORITY_HIGH);
$q->push('l1', QueueInterface::PRIORITY_LOW);
$q->push('n4');

$i = 6;

while ($i--) {
    var_dump($q->pop());
    usleep(50000);
}
```

run `php examples/queue.php`. output:

```
% php examples/queue.php                                                                                                                                                     17-06-11 - 22:36:01
driver is sysv
string(2) "h1"
string(2) "n1"
string(2) "n2"
array(1) {
  [0] =>
  string(11) "n3-array-value"
}
string(2) "n2"
string(2) "l1"
```

## License

MIT
