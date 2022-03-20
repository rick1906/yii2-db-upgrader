<?php

/*
 * Copyright (c) 2021 Rick
 *
 * This file is a part of a project 'megabike'.
 * MIT licence.
 */

namespace megabike\yii2\migration;

use yii\db\Migration;

/**
 *
 * @author Rick
 */
class VersionedMigration extends Migration implements \ArrayAccess
{

    /**
     * 
     * @var mixed
     */
    public $version = null;

    /**
     * 
     * @var string
     */
    public $table = null;

    /**
     * 
     * @var callable
     */
    public $up = null;

    /**
     * 
     * @var callable
     */
    public $down = null;

    /**
     * 
     * @var bool
     */
    public $useTransaction = true;

    /**
     * 
     * @var bool
     */
    public $ignoreErrors = false;

    /**
     * 
     * @var string|null
     */
    public $comment = null;

    /**
     * 
     * @var bool
     */
    public $throw = true;

    /**
     * 
     * @var bool
     */
    public $output = true;

    /**
     * 
     * @var array
     */
    protected static $migrationMethods = array(
        'execute',
        'insert',
        'batchInsert',
        'upsert',
        'update',
        'delete',
        'createTable',
        'renameTable',
        'dropTable',
        'truncateTable',
        'addColumn',
        'dropColumn',
        'renameColumn',
        'alterColumn',
        'addPrimaryKey',
        'dropPrimaryKey',
        'addForeignKey',
        'dropForeignKey',
        'createIndex',
        'dropIndex',
        'addCommentOnColumn',
        'addCommentOnTable',
        'dropCommentFromColumn',
        'dropCommentFromTable',
    );

    public function __construct($config = [], $up = null, $down = null)
    {
        if (is_scalar($config)) {
            $config = array('version' => $config);
        }
        if ($up !== null) {
            $config['up'] = $up;
        }
        if ($down !== null) {
            $config['down'] = $down;
        }
        parent::__construct($config);
    }

    public function getVersion()
    {
        return $this->version;
    }

    public function getUseTransaction()
    {
        return $this->useTransaction;
    }

    protected function isPredefinedMigration($name)
    {
        return in_array($name, self::$migrationMethods);
    }

    protected function buildReverseMigration($name, $arguments)
    {
        return false;
    }

    /**
     * 
     * @param mixed $migration see apply method
     * @return bool
     */
    protected function applyReverse($migration)
    {
        if ($migration === false) {
            return false;
        }
        if (!$migration) {
            return null;
        }
        if (is_array($migration) && isset($migration[0])) {
            $name = $migration[0];
            if (is_array($name)) {
                return $this->applyMultiple($migration, true);
            }
            if ($this->isPredefinedMigration($name)) {
                $args = array_slice($migration, 1);
                $reverse = $this->buildReverseMigration($name, $args);
                return $this->apply($reverse);
            }
        }
        return false;
    }

    /**
     * 
     * @param mixed $migration a) predefined [name, ...args] b) multiple [[up,dn], [up,dn], ...] c) callable|command|updater
     * @param mixed $reverse
     * @return bool|null
     */
    protected function apply($migration, $reverse = null)
    {
        if ($migration === true) {
            return $this->applyReverse($reverse);
        }
        if ($migration === false) {
            return false;
        }
        if (!$migration) {
            return null;
        }
        if (is_array($migration) && isset($migration[0])) {
            $name = $migration[0];
            if (is_array($name)) {
                return $this->applyMultiple($migration);
            }
            if (method_exists($this, $name) && $this->isPredefinedMigration($name)) {
                $args = array_slice($migration, 1);
                call_user_func_array(array($this, $name), $args);
                return null;
            }
        }
        if ($migration instanceof TableUpgrader) {
            $time = $this->beginCommand("Executing ".get_class($migration)." for table '".$migration->getRawTableName()."'");
            $migration->refresh();
            $migration->execute();
            $this->endCommand($time);
            return null;
        }
        if ($migration instanceof \yii\db\Command) {
            $time = $this->beginCommand("Executing ".get_class($migration));
            $migration->execute();
            $this->endCommand($time);
            return null;
        }
        if (is_callable($migration)) {
            $time = $this->beginCommand("Executing callable");
            call_user_func($migration, $this->getDb());
            $this->endCommand($time);
            return null;
        }
        return false;
    }

    protected function applyMultiple($migrations, $down = false)
    {
        foreach ($migrations as $pair) {
            if ($down) {
                $result = $this->apply($pair[1], $pair[0]);
            } else {
                $result = $this->apply($pair[0]);
            }
            if ($result === false) {
                return false;
            }
        }
        return null;
    }

    public function up()
    {
        if ($this->useTransaction) {
            return $this->safeUp();
        } else {
            return $this->unsafeUp();
        }
    }

    protected function handleException($ex)
    {
        if ($this->output) {
            echo 'Exception: '.$ex->getMessage().' ('.$ex->getFile().':'.$ex->getLine().")\n";
            echo $ex->getTraceAsString()."\n";
        }
        if ($this->throw && !$this->ignoreErrors) {
            throw $ex;
        }
    }

    protected function beginCommand($description)
    {
        if ($this->output && !$this->compact) {
            echo "    > $description ... ";
        }
        return microtime(true);
    }

    protected function endCommand($time)
    {
        if ($this->output && !$this->compact) {
            echo 'done (time: '.sprintf('%.3f', microtime(true) - $time)."s)\n";
        }
    }

    public function down()
    {
        if ($this->useTransaction) {
            return $this->safeDown();
        } else {
            return $this->unsafeDown();
        }
    }

    public function safeUp()
    {
        $transaction = $this->db->beginTransaction();
        try {
            if ($this->apply($this->up) === false) {
                $transaction->rollBack();
                return false;
            }
            $transaction->commit();
        } catch (\Exception $ex) {
            $transaction->rollBack();
            $this->handleException($ex);
            return false;
        } catch (\Throwable $ex) {
            $transaction->rollBack();
            $this->handleException($ex);
            return false;
        }

        return null;
    }

    public function safeDown()
    {
        $transaction = $this->db->beginTransaction();
        try {
            if ($this->apply($this->down, $this->up) === false) {
                $transaction->rollBack();
                return false;
            }
            $transaction->commit();
        } catch (\Exception $ex) {
            $transaction->rollBack();
            $this->handleException($ex);
            return false;
        } catch (\Throwable $ex) {
            $transaction->rollBack();
            $this->handleException($ex);
            return false;
        }

        return null;
    }

    public function unsafeUp()
    {
        try {
            if ($this->apply($this->up) === false) {
                return false;
            }
        } catch (\Exception $ex) {
            $this->handleException($ex);
            return false;
        } catch (\Throwable $ex) {
            $this->handleException($ex);
            return false;
        }

        return null;
    }

    public function unsafeDown()
    {
        try {
            if ($this->apply($this->down, $this->up) === false) {
                return false;
            }
        } catch (\Exception $ex) {
            $this->handleException($ex);
            return false;
        } catch (\Throwable $ex) {
            $this->handleException($ex);
            return false;
        }

        return null;
    }

    public function offsetExists($offset)
    {
        return $this->__isset($offset);
    }

    public function offsetGet($offset)
    {
        return $this->__get($offset);
    }

    public function offsetSet($offset, $value)
    {
        return $this->__set($offset, $value);
    }

    public function offsetUnset($offset)
    {
        return $this->__unset($offset);
    }

}
