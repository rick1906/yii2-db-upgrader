<?php

/*
 * Copyright (c) 2021 Rick
 *
 * This file is a part of a project 'megabike'.
 * MIT licence.
 */

namespace megabike\yii2\migration;

/**
 *
 * @author Rick
 */
class MigrationBuilder
{

    /**
     * 
     * @var array
     */
    protected $migrations;

    /**
     * 
     * @var mixed
     */
    protected $version;

    /**
     * 
     * @var bool
     */
    protected $multipleTarget = null;

    /**
     * 
     * @var array
     */
    protected $params = [];

    public function __construct($version, &$migrations, $featureSince)
    {
        $this->version = $version;
        $this->migrations = &$migrations;
        if ((string)$featureSince !== '') {
            $this->params['since'] = true;
            if (!is_bool($featureSince)) {
                $this->params['featureId'] = (string)$featureSince;
            }
        }
    }

    /**
     * 
     * @param mixed $version
     * @param callable $up
     * @param callable $down
     * @return $this
     */
    public static function createMigrationConfig($version, $up, $down = false, $params = [])
    {
        $config = ['version' => $version, 'up' => $up, 'down' => $down];
        if ($params) {
            $config += $params;
        }
        return $config;
    }

    protected function newMigration($up, $down = false, $useTransaction = null, $ignoreErrors = null)
    {
        $params = $this->params;
        if ($useTransaction !== null) {
            $params['useTransaction'] = $useTransaction;
        }
        if ($ignoreErrors !== null) {
            $params['ignoreErrors'] = $ignoreErrors;
        }
        return static::createMigrationConfig($this->version, $up, $down, $params);
    }

    protected function setParam($name, $flag, $value)
    {
        if ($flag !== null) {
            $this->params[$name] = $value;
        } else {
            unset($this->params[$name]);
        }
        if ($this->multipleTarget !== null && $flag !== null) {
            $this->multipleTarget[$name] = $value;
        }
        return $this;
    }

    /**
     * 
     * @param bool|null $flag
     * @return $this
     */
    public function unsafe($flag = true)
    {
        return $this->setParam('useTransaction', $flag, !$flag);
    }

    /**
     * 
     * @param bool|null $flag
     * @return $this
     */
    public function safe($flag = true)
    {
        return $this->setParam('useTransaction', $flag, (bool)$flag);
    }

    /**
     * 
     * @param bool|null $flag
     * @return $this
     */
    public function ignoreErrors($flag = true)
    {
        return $this->setParam('ignoreErrors', $flag, (bool)$flag);
    }

    /**
     * 
     * @return $this
     */
    public function transaction()
    {
        $this->multipleTarget = $this->newMigration([], true, true);
        $this->migrations[] = &$this->multipleTarget;
        return $this;
    }

    /**
     * 
     * @param string|null $text
     * @return $this
     */
    public function comment($text = null)
    {
        return $this->setParam('comment', $text, (string)$text);
    }

    /**
     * 
     * @param callable $up
     * @param callable $down
     * @return $this
     */
    public function addMigration($up, $down = false)
    {
        if ($this->multipleTarget !== null && isset($this->multipleTarget['up'])) {
            $this->multipleTarget['up'][] = [$up, $down];
        } else {
            $this->migrations[] = $this->newMigration($up, $down);
        }
        return $this;
    }

    /**
     * 
     * @param string $name
     * @param array $arguments
     * @param bool|null $ignoreErrors
     * @return $this
     */
    public function addPredefinedMigration($name, $arguments, $ignoreErrors = null)
    {
        array_unshift($arguments, $name);
        if ($this->multipleTarget !== null) {
            $this->addMigration($arguments, true);
        } elseif ($ignoreErrors !== null && !isset($this->params['ignoreErrors'])) {
            $this->migrations[] = $this->newMigration($arguments, true, null, $ignoreErrors);
        } else {
            $this->migrations[] = $this->newMigration($arguments, true);
        }
    }

    /**
     * Executes a SQL statement.
     * This method executes the specified SQL statement using [[db]].
     * @param string $sql the SQL statement to be executed
     * @param array $params input parameters (name => value) for the SQL execution.
     * See [[Command::execute()]] for more details.
     */
    public function execute($sql, $params = [])
    {
        return $this->addPredefinedMigration(__FUNCTION__, func_get_args());
    }

}
