<?php

namespace Elasticsearch\ConnectionPool;

use Elasticsearch\Common\Exceptions\NoNodesAvailableException;
use Elasticsearch\ConnectionPool\Selectors\SelectorInterface;
use Elasticsearch\Connections\Connection;
use Elasticsearch\Connections\ConnectionFactoryInterface;

class StaticNoPingConnectionPool extends AbstractConnectionPool implements ConnectionPoolInterface {
  /**
   * @var int
   */
  private $pingTimeout = 60; // 60s???

  /**
   * @var int
   */
  private $maxPingTimeout = 3600; // 似乎太大了

  /**
   * {@inheritdoc}
   */
  public function __construct($connections, SelectorInterface $selector, ConnectionFactoryInterface $factory, $connectionPoolParams) {
    parent::__construct($connections, $selector, $factory, $connectionPoolParams);
  }

  /**
   * @param bool $force
   *
   * @return Connection
   * @throws \Elasticsearch\Common\Exceptions\NoNodesAvailableException
   */
  public function nextConnection($force = false) {
    $total = count($this->connections);

    //
    // 假定我们现在只有一个connection
    //    NoNodesAvailableException
    //    需要关注: isAlive 或者 readyToRevive
    while ($total--) {
      /** @var Connection $connection */

      // 选择一个Connections
      $connection = $this->selector->select($this->connections);

      // 判断是否alive
      if ($connection->isAlive() === true) {
        return $connection;
      }

      // 该connection现在可以一试
      // 1. ping没有过期
      if ($this->readyToRevive($connection) === true) {
        return $connection;
      }
    }

    //
    // 简化逻辑
    // $connection
    // $connection->isAlive()
    // $this->readyToRevive($connection) !== true
    // 然后就失败了
    //
    throw new NoNodesAvailableException("No alive nodes found in your cluster");
  }

  public function scheduleCheck() {
  }

  /**
   * @param \Elasticsearch\Connections\Connection $connection
   *
   * @return bool
   */
  private function readyToRevive(Connection $connection) {
    $timeout = min(
      $this->pingTimeout * pow(2, $connection->getPingFailures()),
      $this->maxPingTimeout
    );

    if ($connection->getLastPing() + $timeout < time()) {
      return true;
    } else {
      return false;
    }
  }
}
