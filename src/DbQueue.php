<?php
/**
 * Created by PhpStorm.
 * User: inhere
 * Date: 2017/5/21
 * Time: 上午1:45
 */

namespace inhere\queue;

/**
 * Class DbQueue
 * @package inhere\queue
 */
class DbQueue extends BaseQueue
{
    /**
     * @var string
     */
    protected $driver = QueueFactory::DRIVER_DB;

    /**
     * @var \PDO
     */
    private $db;

    /**
     * @var string
     */
    private $tableName = 'msg_queue';

    /**
     * @var array
     */
    protected $config = [
        'id' => null,
        'serialize' => true,
        'tableName' => '',
    ];

    /**
     * DbQueue constructor.
     * @param array $config
     */
    public function __construct(array $config = [])
    {
        if (isset($config['db'])) {
            $this->setDb($config['db']);
            unset($config['db']);
        }

        parent::__construct($config);

        if (!empty($this->config['tableName'])) {
            $this->setTableName($this->config['tableName']);
        }

        if (!$this->id) {
            $this->id = $this->driver;
        }
    }

    /**
     * {@inheritdoc}
     */
    protected function doPush($data, $priority = self::PRIORITY_NORM)
    {
        return $this->db->exec(sprintf(
            "INSERT INTO %s (`queue`, `data`, `priority`, `created_at`) VALUES (%s, %s, %d, %d)",
            $this->tableName,
            $this->id,
            $this->encode($data),
            $priority,
            time()
        ));
    }

    /**
     * {@inheritDoc}
     */
    protected function doPop()
    {
        $st = $this->db->query(sprintf(
            "SELECT `id`,`data` FROM %s WHERE queue = %s ORDER BY `priority` DESC, `id` ASC LIMIT 1",
            $this->tableName,
            $this->id
        ));

        if ($row = $st->fetch(\PDO::FETCH_ASSOC)) {
            $row['data'] = $this->decode($row['data']);
        }

        return $row;
    }

    /**
     * {@inheritDoc}
     */
    protected function close()
    {
        parent::close();

        $this->db = null;
    }

    /**
     * @return string
     */
    public function getTableName(): string
    {
        return $this->tableName;
    }

    /**
     * @param string $tableName
     */
    public function setTableName(string $tableName)
    {
        $this->tableName = $tableName;
    }

    /**
     * @return \PDO
     */
    public function getDb(): \PDO
    {
        return $this->db;
    }

    /**
     * @param \PDO $db
     */
    public function setDb(\PDO $db)
    {
        $this->db = $db;
    }

    /**
     *
     * ```php
     * $dqe->createTable($dqe->createMysqlTableSql());
     * ```
     * @param string $sql
     * @return int
     */
    public function createTable($sql)
    {
        return $this->db->exec($sql);
    }

    /**
     * @return string
     */
    public function createMysqlTableSql()
    {
        $tName = $this->tableName;
        return <<<EOF
CREATE TABLE IF NOT EXISTS `$tName` (
	`id` INT(11) NOT NULL AUTO_INCREMENT,
	`queue` CHAR(48) NOT NULL COMMENT 'queue name', 
	`data` TEXT NOT NULL COMMENT 'task data',
	`priority` TINYINT(2) UNSIGNED NOT NULL DEFAULT 1,
	`created_at` INT(10) UNSIGNED NOT NULL,
	`started_at` INT(10) UNSIGNED NOT NULL DEFAULT 0,
	`finished_at` INT(10) UNSIGNED NOT NULL DEFAULT 0,
	KEY (`queue`, `created_at`),
	PRIMARY KEY (`iId`)
)
COLLATE='utf8_general_ci'
ENGINE=InnoDB
EOF;
    }

    /**
     * @return int
     */
    public function createSqliteTableSql()
    {
        $tName = $this->tableName;
        return <<<EOF
CREATE TABLE IF NOT EXISTS `$tName` (
	`id` INTEGER PRIMARY KEY NOT NULL,
	`queue` CHAR(48) NOT NULL COMMENT 'queue name', 
	`data` TEXT NOT NULL COMMENT 'task data',
	`priority` INTEGER(2) NOT NULL DEFAULT 1,
	`created_at` INTEGER(10) NOT NULL,
	`started_at` INTEGER(10) NOT NULL DEFAULT 0,
	`finished_at` INTEGER(10) NOT NULL DEFAULT 0
);
CREATE INDEX idxQueue on $tName(queue);
CREATE INDEX idxCreatedAt on $tName(created_at);
EOF;
    }
}
