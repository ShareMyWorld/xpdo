<?php

if (!empty($this->config['charset'])) {
    $sql = "SET NAMES '{$this->config['charset']}'";
    $this->pdo->exec($sql);
}
