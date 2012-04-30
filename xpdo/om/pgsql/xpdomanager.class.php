<?php

/**
 * Include the parent {@link xPDOManager} class.
 */
require_once (dirname(dirname(__FILE__)) . '/xpdomanager.class.php');

/**
 * Provides PostgreSQL data source management for an xPDO instance.
 *
 * These are utility functions that only need to be loaded under special
 * circumstances, such as creating tables, adding indexes, altering table
 * structures, etc.  xPDOManager class implementations are specific to a
 * database driver and this instance is implemented for PostgreSQL.
 *
 * @package xpdo
 * @subpackage om.pgsql
 */

class xPDOManager_pgsql extends xPDOManager {
    public function createSourceContainer($dsnArray = null, $username = null, $password = null, $containerOptions = array()) {
        $created = false;
        if ($this->xpdo->getConnection(array(xPDO::OPT_CONN_MUTABLE => true))) {
            if ($dsnArray === null) {
                $dsnArray = xPDO::parseDSN($this->xpdo->getOption('dsn'));
            }
            if ($username === null) {
                $username = $this->xpdo->getOption('username', null, '');
            }
            if ($password === null) {
                $password = $this->xpdo->getOption('password', null, '');
            }
            if (is_array($dsnArray) && is_string($username) && is_string($password)) {
                // TODO @addeventure: Escaping?
                // For the moment, we always use template0 to avoid as many problems as possible with character sets.
                // This should perhaps go to config at some point, or some would argue that using template1 is the good thing to do
                // if collation and charset is not set.
                $sql = 'CREATE DATABASE "' . $dsnArray['dbname'] . '" TEMPLATE template0';
                
                if (isset($containerOptions['collation']) && isset($containerOptions['charset'])) {
                    $sql .= " ENCODING '{$containerOptions['charset']}'";
                    $sql .= " LC_COLLATE '{$containerOptions['collation']}'";
                    $sql .= " LC_CTYPE '{$containerOptions['collation']}'";
                }
                
                try {
                    $pdo = new PDO("pgsql:host={$dsnArray['host']}", $username, $password, array(PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION));
                    $result = $pdo->exec($sql);
                    if ($result !== false) {
                        $created = true;
                    } else {
                        $this->xpdo->log(xPDO::LOG_LEVEL_ERROR, "Could not create source container:\n{$sql}\nresult = " . var_export($result, true));
                    }
                } catch (PDOException $pe) {
                    $this->xpdo->log(xPDO::LOG_LEVEL_ERROR, "Could not connect to database server: " . $pe->getMessage());
                } catch (Exception $e) {
                    $this->xpdo->log(xPDO::LOG_LEVEL_ERROR, "Could not create source container: " . $e->getMessage());
                }
            }
        } else {
            $this->xpdo->log(xPDO::LOG_LEVEL_ERROR, "Could not get writable connection", '', __METHOD__, __FILE__, __LINE__);
        }
        return $created;
    }

    public function removeSourceContainer($dsnArray = null, $username = null, $password = null) {
        $removed = false;
        if ($this->xpdo->getConnection(array(xPDO::OPT_CONN_MUTABLE => true))) {
            if ($dsnArray === null) {
                $dsnArray = xPDO::parseDSN($this->xpdo->getOption('dsn'));
            }
            if ($username === null) {
                $username = $this->xpdo->getOption('username', null, '');
            }
            if ($password === null) {
                $password = $this->xpdo->getOption('password', null, '');
            }
            if (is_array($dsnArray) && is_string($username) && is_string($password)) {
                $sql = 'DROP DATABASE ' . $this->xpdo->escape($dsnArray['dbname']);
                try {
                    $pdo = new PDO("pgsql:host={$dsnArray['host']}", $username, $password, array(PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION));
                    $result = $pdo->exec($sql);
                    if ($result !== false) {
                        $removed = true;
                    } else {
                        $this->xpdo->log(xPDO::LOG_LEVEL_ERROR, "Could not remove source container:\n{$sql}\nresult = " . var_export($result, true));
                    }
                } catch (PDOException $pe) {
                    $this->xpdo->log(xPDO::LOG_LEVEL_ERROR, "Could not connect to database server: " . $pe->getMessage());
                } catch (Exception $e) {
                    $this->xpdo->log(xPDO::LOG_LEVEL_ERROR, "Could not remove source container: " . $e->getMessage());
                }
            }
        } else {
            $this->xpdo->log(xPDO::LOG_LEVEL_ERROR, "Could not get writable connection", '', __METHOD__, __FILE__, __LINE__);
        }
        return $removed;
    }

    public function removeObjectContainer($className) {
        $removed= false;
        if ($this->xpdo->getConnection(array(xPDO::OPT_CONN_MUTABLE => true))) {
            $instance= $this->xpdo->newObject($className);
            if ($instance) {
                $sql = 'DROP TABLE ' . $this->xpdo->getTableName($className);
                $removed= $this->xpdo->exec($sql);
                if ($removed === false && $this->xpdo->errorCode() !== '' && $this->xpdo->errorCode() !== PDO::ERR_NONE) {
                    $this->xpdo->log(xPDO::LOG_LEVEL_ERROR, 'Could not drop table ' . $className . "\nSQL: {$sql}\nERROR: " . print_r($this->xpdo->pdo->errorInfo(), true));
                } else {
                    $removed= true;
                    $this->xpdo->log(xPDO::LOG_LEVEL_INFO, 'Dropped table' . $className . "\nSQL: {$sql}\n");
                }
            }
        } else {
            $this->xpdo->log(xPDO::LOG_LEVEL_ERROR, "Could not get writable connection", '', __METHOD__, __FILE__, __LINE__);
        }
        return $removed;
    }

    public function createObjectContainer($className) {
        $created = false;
        if ($this->xpdo->getConnection(array(xPDO::OPT_CONN_MUTABLE => true))) {
            $instance = $this->xpdo->newObject($className);
            if ($instance) {
                $tableName = $this->xpdo->getTableName($className);

                // TODO: SELECT COUNT(*) is really slow in pgsql and requires a table scan 
                // which could potentially be reeally slow and demanding on the server.
                $existsStmt = $this->xpdo->query("SELECT COUNT(*) FROM  $tableName");
                if ($existsStmt && $existsStmt->fetchAll()) {
                    return true;
                }
                
                $sql = 'CREATE TABLE ' . $tableName . ' (';
                $fieldMeta = $this->xpdo->getFieldMeta($className, true);
                $columns = array();
                
                foreach ($fieldMeta as $key => $meta) {
                    $columns[] = $this->getColumnDef($className, $key, $meta);
                    //if (array_key_exists('generated', $meta) && $meta['generated'] == 'native') $nativeGen = true;
                }
                $sql .= implode(', ', $columns);
                
                $indexes = $this->xpdo->getIndexMeta($className);
                $tableConstraints = array();
                $indexStatements = array();
                foreach ($indexes as $indexName => $indexMeta) {
                    $indexDef = $this->getIndexDef($className, $indexName, $indexMeta);
                    if ($this->isTableConstraint($indexMeta)) {
                        $tableConstraints[] = $indexDef;
                    } else {
                        $indexStatements[$indexName] = $indexStatements;
                    }
                }
                
                if (!empty($tableConstraints)) {
                    $sql .= ', ' . implode(', ', $tableConstraints);
                }
                $sql .= ")";
               
                $created = $this->xpdo->exec($sql);
                if ($created === false && $this->xpdo->errorCode() !== '' && $this->xpdo->errorCode() !== PDO::ERR_NONE) {
                    $this->xpdo->log(xPDO::LOG_LEVEL_ERROR, 'Could not create table ' . $tableName . "\nSQL: {$sql}\nERROR: " . print_r($this->xpdo->errorInfo(), true));
                } else {
                    $anyIndexCreationFailed = false;
                    foreach ($indexStatements as $indexName => $createIndexStatement) {
                        $indexCreated = $this->xpdo->exec($createIndexStatement);
                        if ($indexCreated === false) {
                            $this->xpdo->log(xPDO::LOG_LEVEL_ERROR, 'Could not create index ' . $indexName . "\nSQL: {$createIndexStatement}\nERROR: " . print_r($this->xpdo->errorInfo(), true));
                            $anyIndexCreationFailed = true;
                            break;
                        }
                    }
                    
                    if (!$anyIndexCreationFailed) {
                        $created = true;
                        $this->xpdo->log(xPDO::LOG_LEVEL_INFO, 'Created table ' . $tableName . "\nSQL: {$sql}\n");
                    }
                }
            }
        } else {
            $this->xpdo->log(xPDO::LOG_LEVEL_ERROR, "Could not get writable connection", '', __METHOD__, __FILE__, __LINE__);
        }
        return $created;
    }

    public function alterObjectContainer($className, array $options = array()) {
        if ($this->xpdo->getConnection(array(xPDO::OPT_CONN_MUTABLE => true))) {
            // TODO: Implement alterObjectContainer() method.
        } else {
            $this->xpdo->log(xPDO::LOG_LEVEL_ERROR, "Could not get writable connection", '', __METHOD__, __FILE__, __LINE__);
        }
    }

    public function addConstraint($class, $name, array $options = array()) {
        if ($this->xpdo->getConnection(array(xPDO::OPT_CONN_MUTABLE => true))) {
            // TODO: Implement addConstraint() method.
        } else {
            $this->xpdo->log(xPDO::LOG_LEVEL_ERROR, "Could not get writable connection", '', __METHOD__, __FILE__, __LINE__);
        }
    }

    public function addField($class, $name, array $options = array()) {
        $result = false;
        if ($this->xpdo->getConnection(array(xPDO::OPT_CONN_MUTABLE => true))) {
            $className = $this->xpdo->loadClass($class);
            if ($className) {
                $meta = $this->xpdo->getFieldMeta($className, true);
                if (isset($meta[$name])) {
                    $colDef = $this->getColumnDef($className, $name, $meta[$name]);
                    if (!empty($colDef)) {
                        $sql = "ALTER TABLE {$this->xpdo->getTableName($className)} ADD COLUMN {$colDef}";
                        
                        if ($this->xpdo->exec($sql) !== false) {
                            $result = true;
                        } else {
                            $this->xpdo->log(xPDO::LOG_LEVEL_ERROR, "Error adding field {$class}->{$name}: " . print_r($this->xpdo->errorInfo(), true), '', __METHOD__, __FILE__, __LINE__);
                        }
                    } else {
                        $this->xpdo->log(xPDO::LOG_LEVEL_ERROR, "Error adding field {$class}->{$name}: Could not get column definition");
                    }
                } else {
                    $this->xpdo->log(xPDO::LOG_LEVEL_ERROR, "Error adding field {$class}->{$name}: No metadata defined");
                }
            }
        } else {
            $this->xpdo->log(xPDO::LOG_LEVEL_ERROR, "Could not get writable connection", '', __METHOD__, __FILE__, __LINE__);
        }
        return $result;
    }

    public function addIndex($class, $name, array $options = array()) {
        $result = false;
        if ($this->xpdo->getConnection(array(xPDO::OPT_CONN_MUTABLE => true))) {
            $className = $this->xpdo->loadClass($class);
            if ($className) {
                $meta = $this->xpdo->getIndexMeta($className);
                if (isset($meta[$name])) {
                    $idxDef = $this->getIndexDef($className, $name, $meta[$name]);
                    if (!empty($idxDef)) {
                        if ($this->isTableConstraint($meta[$name])) {
                            $sql = "ALTER TABLE {$this->xpdo->getTableName($className)} ADD $idxDef";
                        } else {
                            $sql = $idxDef;
                        }
                        
                        if ($this->xpdo->exec($sql) !== false) {
                            $result = true;
                        } else {
                            $this->xpdo->log(xPDO::LOG_LEVEL_ERROR, "Error adding index {$name} to {$class}: " . print_r($this->xpdo->errorInfo(), true), '', __METHOD__, __FILE__, __LINE__);
                        }
                    } else {
                        $this->xpdo->log(xPDO::LOG_LEVEL_ERROR, "Error adding index {$name} to {$class}: Could not get index definition");
                    }
                    
                } else {
                    $this->xpdo->log(xPDO::LOG_LEVEL_ERROR, "Error adding index {$name} to {$class}: No metadata defined");
                }
            }
        } else {
            $this->xpdo->log(xPDO::LOG_LEVEL_ERROR, "Could not get writable connection", '', __METHOD__, __FILE__, __LINE__);
        }
        return $result;
    }

    public function alterField($class, $name, array $options = array()) {
        
        $result = false;
        
        if ($this->xpdo->getConnection(array(xPDO::OPT_CONN_MUTABLE => true))) {
            $className = $this->xpdo->loadClass($class);
            if ($className) {
                
                // TODO: what to do here...?? This will not work as expected. 
                // PostGre SQL requires the user to explicitly define how the conversion should be applied.
                // We might need to parse the column definition and compare it what we already have in order to do the correct action.
                $this->xpdo->log(xPDO::LOG_LEVEL_ERROR, "Error altering field {$class}->{$name}: Not supported");
//                $meta = $this->xpdo->getFieldMeta($className, true);
//                if (is_array($meta) && array_key_exists($name, $meta)) {
//                    $colDef = $this->getColumnDef($className, $name, $meta[$name]);
//                    if (!empty($colDef)) {
//                        
//                        $sql = "ALTER TABLE {$this->xpdo->getTableName($className)} ALTER COLUMN {$colDef}";
//                        
//                        if ($this->xpdo->exec($sql) !== false) {
//                            $result = true;
//                        } else {
//                            $this->xpdo->log(xPDO::LOG_LEVEL_ERROR, "Error altering field {$class}->{$name}: " . print_r($this->xpdo->errorInfo(), true), '', __METHOD__, __FILE__, __LINE__);
//                        }
//                    } else {
//                        $this->xpdo->log(xPDO::LOG_LEVEL_ERROR, "Error altering field {$class}->{$name}: Could not get column definition");
//                    }
//                } else {
//                    $this->xpdo->log(xPDO::LOG_LEVEL_ERROR, "Error altering field {$class}->{$name}: No metadata defined");
//                }
            }
        } else {
            $this->xpdo->log(xPDO::LOG_LEVEL_ERROR, "Could not get writable connection", '', __METHOD__, __FILE__, __LINE__);
        }
        return $result;
    }

    public function removeConstraint($class, $name, array $options = array()) {
        $result = false;
        if ($this->xpdo->getConnection(array(xPDO::OPT_CONN_MUTABLE => true))) {
            $className = $this->xpdo->loadClass($class);
            if ($className) {
                $sql = "ALTER TABLE {$this->xpdo->getTableName($className)} DROP CONSTRAINT {$this->xpdo->escape($name)}";
                $result = $this->xpdo->exec($sql);
                if ($result !== false) {
                    $result = true;
                } else {
                    $this->xpdo->log(xPDO::LOG_LEVEL_ERROR, "Error removing field {$class}->{$name}: " . print_r($this->xpdo->errorInfo(), true), '', __METHOD__, __FILE__, __LINE__);
                }
            }
        } else {
            $this->xpdo->log(xPDO::LOG_LEVEL_ERROR, "Could not get writable connection", '', __METHOD__, __FILE__, __LINE__);
        }
        return $result;
    }

    public function removeField($class, $name, array $options = array()) {
        $result = false;
        if ($this->xpdo->getConnection(array(xPDO::OPT_CONN_MUTABLE => true))) {
            $className = $this->xpdo->loadClass($class);
            if ($className) {
                $sql = "ALTER TABLE {$this->xpdo->getTableName($className)} DROP COLUMN {$this->xpdo->escape($name)}";
                if ($this->xpdo->exec($sql) !== false) {
                    $result = true;
                } else {
                    $this->xpdo->log(xPDO::LOG_LEVEL_ERROR, "Error removing field {$class}->{$name}: " . print_r($this->xpdo->errorInfo(), true), '', __METHOD__, __FILE__, __LINE__);
                }
            }
        } else {
            $this->xpdo->log(xPDO::LOG_LEVEL_ERROR, "Could not get writable connection", '', __METHOD__, __FILE__, __LINE__);
        }
        return $result;
    }

    public function removeIndex($class, $name, array $options = array()) {
        $result = false;
        if ($this->xpdo->getConnection(array(xPDO::OPT_CONN_MUTABLE => true))) {
            $className = $this->xpdo->loadClass($class);
            if ($className) {
                $indexType = (isset($options['type']) ? $options['type'] : 'INDEX');
                switch ($indexType) {
                    case 'PRIMARY KEY':
                    case 'UNIQUE':
                        $sql = "ALTER TABLE {$this->xpdo->getTableName($className)} DROP CONSTRAINT {$this->xpdo->escape($name)}";
                        break;
                    default:
                        $sql = "DROP INDEX {$this->xpdo->escape($name)} ON {$this->xpdo->getTableName($className)}";
                        break;
                }
            }
        } else {
            $this->xpdo->log(xPDO::LOG_LEVEL_ERROR, "Could not get writable connection", '', __METHOD__, __FILE__, __LINE__);
        }
        return $result;
    }

    protected function getColumnDef($class, $name, $meta, array $options = array()) {
        $dbtype = strtoupper($meta['dbtype']);

        $precision = isset($meta['precision']) ? '(' . $meta['precision'] . ')' : '';
        $null = isset($meta['null']) && $meta['null'] === 'false' ? ' NOT NULL' : ' NULL';

        $extra = '';
        if (isset($meta['extra'])) {
            $extra = ' ' . $meta['extra'];
        }
        $phpDateTypes = array('timestamp', 'datetime', 'date');
        $driver = $this->xpdo->driver;
        $dateTimeFunctions = array_merge($driver->_currentDates, $driver->_currentTimes, $driver->_currentTimestamps);
        $default = '';
        if (isset($meta['default'])) {
            $defaultVal = $meta['default'];
            if (strtoupper($defaultVal) === 'NULL'
                    || (in_array($this->xpdo->driver->getPhpType($dbtype), $phpDateTypes) && in_array($defaultVal, $dateTimeFunctions))) {
                $default = " DEFAULT $defaultVal";
            } else {
                $default = " DEFAULT '$defaultVal'";
            }
        }
        $attributes = isset($meta['attributes']) ? ' ' . $meta['attributes'] : '';
        $result = $this->xpdo->escape($name) . ' ' . $dbtype . $precision . $null . $default . $attributes . $extra;
        
        return $result;
    }
    
    private function isTableConstraint($indexMeta) {
        return isset($indexMeta['primary']) || isset($indexMeta['unique']);
    }

    /**
     * Returns the index definition differently depending in the type of index.
     * Primary key and unique will be returned as constraints that can be used in CREATE TABLE / ALTER TABLE statements.
     * All other type of indexes will be returned as index definitions that can be used in CREATE INDEX statements.
     * 
     * @param type $class
     * @param type $name
     * @param type $meta
     * @param array $options
     * @return string  
     */
    protected function getIndexDef($class, $name, $meta, array $options = array()) {
        $result = '';
        
        if (isset($meta['primary'])) {
            $indexType = 'PRIMARY KEY';
        } else if (isset($meta['unique'])) {
            $indexType = 'UNIQUE';
        } else if (isset($meta['type']) && $meta['type'] === 'FULLTEXT') {
            // TODO: What can we do here?
            $indexType = 'INDEX';
        } else {
            $indexType = 'INDEX';
        }
        
        if (!empty($meta['columns'])) {
            $allIndexMembers = array();
            foreach ($meta['columns'] as $indexmember => $unused) {
                $allIndexMembers[] = $this->xpdo->escape($indexmember);
            }
            
            $indexMembersString = implode(',', $allIndexMembers);
            if (!empty($indexMembersString)) {
                if ($this->isTableConstraint($meta)) {
                    // As CREATE/ALTER TABLE  column constraint
                    $result = "CONSTRAINT {$this->xpdo->escape($name)} {$indexType} ({$indexMembersString})";
                } else {
                    $result = "CREATE INDEX {$this->xpdo->escape($name)} ON {$this->xpdo->getTableName($class)} ({$indexMembersString})";
                }
            }
        }
        return $result;
    }
}
