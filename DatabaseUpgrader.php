<?php

/*
 * Copyright (c) 2021 Rick
 *
 * This file is a part of a project 'megabike'.
 * MIT licence.
 */

namespace megabike\yii2\migration;

use yii\base\Component;
use yii\db\Connection;
use yii\di\Instance;

/**
 *
 * @author Rick
 */
class DatabaseUpgrader extends Component implements \yii\base\BootstrapInterface
{

    /**
     * 
     * @var string
     */
    public $upgraderTableName = '{{%database_upgrades}}';

    /**
     * 
     * @var bool
     */
    public $autoUpgrade = false;

    /**
     * 
     * @var bool
     */
    public $addLogFileTarget = true;

    /**
     * 
     * @var string
     */
    public $logCategory = 'dbUpgrader';

    /**
     * 
     * @var string|array
     */
    public $logFile = ['@logs/upgrader.log', '@runtime/logs/upgrader.log'];

    /**
     * 
     * @var array
     */
    public $definitionFiles = [];

    /**
     *
     * @var array
     */
    public $definitionDirs = [];

    /**
     * 
     * @var mixed
     */
    public $definitionInclude = ['*.php'];

    /**
     * 
     * @var bool
     */
    public $definitionDirsRecursive = false;

    /**
     * 
     * @var string|array|Connection
     */
    protected $db = 'db';

    /**
     * 
     * @var bool
     */
    protected $upgraderTableReady = false;

    /**
     * 
     * @var bool
     */
    protected $logReady = false;

    /**
     * 
     * @var array|null
     */
    protected $tableVersionsCache = null;

    public function bootstrap($app)
    {
        if ($this->autoUpgrade) {
            $this->upgrade();
        }
    }

    public function getDb()
    {
        if (!($this->db instanceof Connection)) {
            $this->db = Instance::ensure($this->db, Connection::className());
        }
        return $this->db;
    }

    public function setDb($value)
    {
        $this->db = $value;
    }

    protected function log($message)
    {
        return \Yii::info($message, $this->logCategory);
    }

    public function upgrade($force = false)
    {
        $this->ensureLog();
        $newVersion = $this->getNewGlobalVersion();
        $oldVersion = $this->getGlobalVersion();
        if ($newVersion === false) {
            if (YII_DEBUG) {
                $this->log('No definitions files found.');
            }
            return false;
        }
        if ($force || $this->versionIsGreater($newVersion, $oldVersion)) {
            return $this->runGlobalUpgrade($newVersion, $oldVersion);
        }
        return false;
    }

    protected function getNewGlobalVersion()
    {
        $files = $this->getTableDefinitionFiles();
        $timeUpdated = false;
        foreach ($files as $file) {
            if (is_file($file)) {
                $mtime = filemtime($file);
                if ($mtime !== false && ($timeUpdated === false || $mtime > $timeUpdated)) {
                    $timeUpdated = $mtime;
                }
            }
        }
        return $timeUpdated;
    }

    protected function ensureLog()
    {
        if (!$this->logReady) {
            $this->logReady = true;
            if ($this->addLogFileTarget && $this->logFile !== null) {
                $this->addLogFileTarget($this->logFile);
            }
        }
        return $this->logReady;
    }

    protected function addLogFileTarget($logFile)
    {
        $target = new \yii\log\FileTarget();
        if (is_array($logFile)) {
            foreach ($logFile as $lfv) {
                $file = \Yii::getAlias($lfv, false);
                if ($file !== false) {
                    $target->logFile = $file;
                    break;
                }
            }
        } else {
            $target->logFile = \Yii::getAlias($logFile);
        }
        $target->categories = [$this->logCategory];
        $target->logVars = [];
        \Yii::$app->getLog()->targets[] = $target;
    }

    protected function logResult($result)
    {
        $output = isset($result['output']) ? (string)$result['output'] : '';
        $ex = isset($result['exception']) ? (string)$result['exception'] : '';
        unset($result['output']);
        unset($result['exception']);
        if (empty($result['success'])) {
            $this->log("Upgrade finished with error.");
        }
        if ($ex) {
            $this->log($ex);
        }
        if ($output !== '') {
            $this->log("Output:\n".rtrim($output));
        }
        $this->log($result);
    }

    protected function runGlobalUpgrade($newVersion, $oldVersion)
    {
        $this->ensureLog();
        $results = [];
        $this->log("Running ".get_class($this)." to upgrade from global version '{$oldVersion}' to '{$newVersion}'...");
        $files = $this->getTableDefinitionFiles();
        $this->log("Found ".count($files)." files to search for table definitions.");
        $structures = $this->getTableStructuresFromFiles($files);
        $this->log("Found ".count($structures)." table definitions.");
        $success = true;
        $errors = [];
        foreach ($structures as $key => $ts) {
            $this->log("Running table upgrade for table '".$ts->getTable()."'...");
            $result = $this->upgradeTable($ts);
            if ($result) {
                $results[] = $result;
                if (empty($result['success'])) {
                    $success = false;
                    $errors[$key] = $ts;
                    unset($structures[$key]);
                    $this->log("Failed upgrading table '".$ts->getTable()."'.");
                } else {
                    $this->log("Done upgrading table '".$ts->getTable()."'.");
                }
                $this->logResult($result);
            } else {
                $this->log("Upgrading table '".$ts->getTable()."' is not needed.");
            }
        }
        foreach ($structures as $key => $ts) {
            $this->log("Running deferred upgrades for table '".$ts->getTable()."'...");
            $result = $this->applyDeferredUpgrades($ts);
            if ($result) {
                $results[] = $result;
                if (empty($result['success'])) {
                    $success = false;
                    $errors[$key] = $ts;
                    unset($structures[$key]);
                    $this->log("Failed running deferred upgrades for table '".$ts->getTable()."'.");
                } else {
                    $this->log("Done running deferred upgrades for table '".$ts->getTable()."'.");
                }
                $this->logResult($result);
            } else {
                $this->log("No deferred upgrades for table '".$ts->getTable()."' found.");
            }
        }
        $this->setGlobalVersion($newVersion, $success);
        if ($success) {
            $this->log("Done upgrading to global version '{$newVersion}' without errors.");
        } else {
            $this->log("Done upgrading to global version '{$newVersion}' with errors.");
            $this->log("Upgrade failed for ".count($errors)." tables.");
        }
        return $results;
    }

    protected function getTableStructuresFromFiles($files)
    {
        $structures = [];
        foreach ($files as $file) {
            $classes = $this->getClassesFromFile($file);
            if ($classes) {
                foreach ($classes as $class) {
                    try {
                        $definitions = $this->getTableStructuresFromClass($class);
                    } catch (\Exception $ex) {
                        \Yii::$app->getErrorHandler()->logException($ex);
                        $definitions = false;
                    }
                    if ($definitions) {
                        foreach ($definitions as $item) {
                            $structures[] = $item;
                        }
                    }
                }
            }
        }
        return $structures;
    }

    protected function getTableStructuresFromClass($class)
    {
        if (is_a($class, __NAMESPACE__.'\\TableDefiner', true)) {
            $rc = new \ReflectionClass($class);
            if (!$rc->isAbstract()) {
                return $this->transformTableDefinitions($rc->newInstance($this));
            }
            return false;
        }
        if (is_a($class, __NAMESPACE__.'\\TableDefinerInterface', true)) {
            $callable = array($class, 'getTableDefinitions');
            $definitions = call_user_func($callable);
            return $this->transformTableDefinitions($definitions);
        }
        return false;
    }

    protected function transformTableDefinitions($definitions)
    {
        if ($definitions instanceof TableStructure) {
            return [$definitions];
        }
        if ($definitions instanceof TableDefiner) {
            return $definitions->getDefinitions();
        }

        $items = [];
        foreach ($definitions as $item) {
            if ($item instanceof TableStructure) {
                $items[] = $item;
            }
        }
        return $items;
    }

    protected function getClassesFromFile($file)
    {
        return \megabike\utils\ClassExractor::getClassesFromFile($file);
    }

    protected function getTableDefinitionFiles()
    {
        $files = [];
        $dirs = [];
        if ($this->definitionFiles) {
            if (is_array($this->definitionFiles)) {
                foreach ($this->definitionFiles as $file) {
                    $files[] = \Yii::getAlias($file);
                }
            } else {
                $files[] = \Yii::getAlias((string)$this->definitionFiles);
            }
        }
        if ($this->definitionDirs) {
            if (is_array($this->definitionDirs)) {
                foreach ($this->definitionDirs as $dir) {
                    $dirs[] = \Yii::getAlias($dir);
                }
            } else {
                $dirs[] = \Yii::getAlias((string)$this->definitionDirs);
            }
        }

        $options = ['only' => $this->definitionInclude, 'recursive' => $this->definitionDirsRecursive];
        foreach ($dirs as $dir) {
            if (is_dir($dir)) {
                $found = \yii\helpers\FileHelper::findFiles($dir, $options);
                $files = array_merge($files, $found);
            }
        }

        return $files;
    }

    public function setGlobalVersion($version, $success = true)
    {
        return $this->setTableVersion('', $version, $version, $success);
    }

    public function getGlobalVersion()
    {
        return $this->getTableVersion('');
    }

    public function createTableStructure($table, $db = null)
    {
        $config = ['dbUpgrader' => $this];
        if ($db !== null) {
            $config['db'] = $db;
        }
        return new TableStructure($table, $config);
    }

    protected function generateUpgraderTableStructure()
    {
        $ts = $this->createTableStructure($this->upgraderTableName, $this->getDb());
        $ts->defineVersion(1.01);
        $ts->defineColumns([
            'table' => [$ts->string(144)->notNull(), 'primary' => true],
            'curr_version' => $ts->string(128),
            'temp_version' => $ts->string(128),
            'checked' => $ts->tinyInteger(1),
            'updated' => $ts->integer(),
        ]);
        return $ts;
    }

    public function getTableVersionsInfo($table)
    {
        if (isset($this->tableVersionsCache[$table])) {
            return $this->tableVersionsCache[$table];
        } else {
            $this->ensureUpgraderTable();
            $db = $this->getDb();
            $where = ['table' => $table];
            $row = (new \yii\db\Query)->select('*')->from($this->upgraderTableName)->where($where)->one($db);
            if ($row) {
                $this->tableVersionsCache[$table] = $row;
                return $row;
            } else {
                return $where + [
                    'curr_version' => null,
                    'temp_version' => null,
                    'checked' => null,
                    'updated' => null,
                ];
            }
        }
    }

    public function getTableVersions($table)
    {
        $info = $this->getTableVersionsInfo($table);
        return [$info['curr_version'], $info['temp_version'], !empty($info['checked'])];
    }

    public function getTableVersion($table)
    {
        $data = $this->getTableVersions($table);
        return $data ? $data[0] : null;
    }

    protected function setTableVersions($table, $data)
    {
        $this->ensureUpgraderTable();
        $db = $this->getDb();
        $where = ['table' => $table];
        $data['updated'] = time();
        if (isset($this->tableVersionsCache[$table])) {
            $row = $this->tableVersionsCache[$table];
        } else {
            $row = (new \yii\db\Query)->select('*')->from($this->upgraderTableName)->where($where)->one($db);
        }
        if ($row) {
            $result = $db->createCommand()->update($this->upgraderTableName, $data, $where)->execute();
        } else {
            $data['table'] = $table;
            $result = $db->createCommand()->insert($this->upgraderTableName, $data)->execute();
        }
        if ($row) {
            $this->tableVersionsCache[$table] = array_merge($row, $data);
        } else {
            $this->tableVersionsCache[$table] = $data;
        }
        return $result;
    }

    protected function setTableVersion($table, $currVersion = false, $tempVersion = false, $checked = null)
    {
        $data = [];
        if ($currVersion !== false) {
            $data['curr_version'] = $currVersion;
        }
        if ($tempVersion !== false) {
            $data['temp_version'] = $tempVersion;
        }
        if ($checked !== null) {
            $data['checked'] = (int)$checked;
        }
        if ($data) {
            return $this->setTableVersions($table, $data);
        }
        return 0;
    }

    protected function upgradeUpgraderTable()
    {
        $ts = $this->generateUpgraderTableStructure();
        return $ts->getUpgrader()->execute();
    }

    protected function ensureUpgraderTable()
    {
        if (!$this->upgraderTableReady) {
            $this->upgraderTableReady = true;
            $this->upgradeUpgraderTable();
        }
        return $this->upgraderTableReady;
    }

    public function resetTableVersionsCache()
    {
        $this->tableVersionsCache = null;
    }

    public function upgradeTable(TableStructure $ts, $force = false)
    {
        $key = $ts->getUpgrader()->getRawTableName();
        list($oldVersion, $tempVersion, $checked) = $this->getTableVersions($key);
        $newVersion = $ts->getVersion();
        if ($force || $this->versionIsGreater($newVersion, $oldVersion) || !$checked) {
            return $this->upgradeTableFromVersion($oldVersion, $ts, $tempVersion);
        }
        return false;
    }

    public function upgradeTableFromVersion($oldVersion, TableStructure $ts, $tempVersion = null)
    {
        $newVersion = $ts->getVersion();
        if ($newVersion === null) {
            return false;
        }

        $enabled = $ts->isEnabled();
        $upgrader = $ts->getUpgrader();
        $create = $upgrader->needsCreate();
        $migrations = $this->extractMigrations($ts->getMigrations(), $newVersion, $oldVersion, $ts->isOrderNewerFirst());
        if ($create || $oldVersion == null) {
            if ($create) {
                $tempVersion = true;
            }
            if ($enabled) {
                return $this->runTableCreateMigrations($upgrader, $migrations, $newVersion, $tempVersion);
            } else {
                return $this->runTableUpgradeMigrations([], $newVersion, $oldVersion, $tempVersion);
            }
        } else {
            return $this->runTableUpgradeMigrations($upgrader, $migrations, $newVersion, $oldVersion, $tempVersion);
        }
    }

    public function applyDeferredUpgrades(TableStructure $ts, $force = false)
    {
        $key = $ts->getUpgrader()->getRawTableName().'::deferred';
        list($oldVersion, $tempVersion, $checked) = $this->getTableVersions($key);
        $newVersion = $ts->getVersion();
        if ($force || $this->versionIsGreater($newVersion, $oldVersion) || !$checked) {
            return $this->applyDeferredUpgradesFromVersion($oldVersion, $ts, $tempVersion);
        }
        return false;
    }

    public function applyDeferredUpgradesFromVersion($oldVersion, TableStructure $ts, $tempVersion = null)
    {
        $table = $ts->getUpgrader()->getRawTableName();
        $enabled = $ts->isEnabled();
        $newVersion = $ts->getVersion();
        $deferred = $ts->getDeferredMigrations();
        if ($deferred && $newVersion !== null) {
            $migrations = $this->extractMigrations($deferred, $newVersion, $oldVersion, $ts->isOrderNewerFirst());
            return $this->runDeferedMigrations($table, $migrations, $newVersion, $oldVersion, $tempVersion, $enabled);
        }
        return false;
    }

    /**
     * 
     * @param TableUpgrader $tableUpgrader
     * @param array $migrations
     * @param mixed $newVersion
     * @param mixed $oldVersion
     * @param mixed $tempVersion
     */
    public function runTableCreateMigrations($tableUpgrader, $migrations, $newVersion, $tempVersion)
    {
        $table = $tableUpgrader->getRawTableName();
        $allowErrors = false;
        if ($tempVersion === true) { // table is new
            $tempVersion = null;
        } elseif ($tempVersion === null || $tempVersion === false) { // version is unknown, try run all upgrades
            $tempVersion = null;
            $allowErrors = true;
        } else { // recovered after failed upgrade at tempver
            $migrations = $this->filterMigrationsMinVersion($migrations, $tempVersion);
        }

        $result = ['table' => $table, 'output' => '', 'newVersion' => $newVersion, 'oldVersion' => null, 'tempVersion' => $tempVersion];
        $success = $this->runTableUpgrader($tableUpgrader, $result, $result['output']);

        if ($success) {
            $this->setTableVersion($table, null, $tempVersion, true);
            $success = $this->runTableMigrations($table, $migrations, false, $tempVersion, $allowErrors, $result, $result['output']);
            if ($success) {
                $this->setTableVersion($table, $newVersion, $newVersion);
            }
        }

        $result['success'] = $success;
        return $result;
    }

    /**
     * 
     * @param TableUpgrader $tableUpgrader
     * @param array $migrations
     * @param mixed $newVersion
     * @param mixed $oldVersion
     * @param mixed $tempVersion
     * @return bool
     */
    public function runTableUpgradeMigrations($tableUpgrader, $migrations, $newVersion, $oldVersion, $tempVersion)
    {
        $table = $tableUpgrader->getRawTableName();
        $allowErrors = false;
        if (is_bool($tempVersion)) { // no info on tempver, run as usual
            $tempVersion = null;
        } elseif ($tempVersion !== null) { // recovered after failed upgrade at tempver
            $migrations = $this->filterMigrationsMinVersion($migrations, $tempVersion);
        }

        $result = ['table' => $table, 'output' => '', 'newVersion' => $newVersion, 'oldVersion' => $oldVersion, 'tempVersion' => $tempVersion];
        $success = $this->runTableMigrations($table, $migrations, $oldVersion, $tempVersion, $allowErrors, $result, $result['output']);
        if ($success) {
            $versionChanged = $this->versionIsGreater($newVersion, $oldVersion);
            $checked = !$migrations && !$versionChanged && $this->getTableVersions($table)[2];
            $this->setTableVersion($table, $newVersion, $newVersion, $checked);
            if (!$checked) {
                $tableUpgrader->refresh();
                $success = $this->runTableUpgrader($tableUpgrader, $result, $result['output']);
                if ($success) {
                    $this->setTableVersion($table, false, false, true);
                }
            }
        }

        $result['success'] = $success;
        return $result;
    }

    /**
     * 
     * @param string $table
     * @param array $migrations
     * @param mixed $newVersion
     * @param mixed $oldVersion
     * @param mixed $tempVersion
     * @return bool
     */
    public function runDeferedMigrations($table, $migrations, $newVersion, $oldVersion, $tempVersion, $enabled)
    {
        $allowErrors = !$enabled;
        if (is_bool($tempVersion)) { // no info on tempver, run as usual
            $tempVersion = null;
        } elseif ($tempVersion !== null) { // recovered after failed upgrade at tempver
            $migrations = $this->filterMigrationsMinVersion($migrations, $tempVersion);
        }

        $key = $table.'::deferred';
        $result = ['table' => $table, 'output' => '', 'newVersion' => $newVersion, 'oldVersion' => $oldVersion, 'tempVersion' => $tempVersion];
        $success = $this->runTableMigrations($key, $migrations, $oldVersion, $tempVersion, $allowErrors, $result, $result['output']);
        if ($success) {
            $this->setTableVersion($key, $newVersion, $newVersion, true);
        }

        $result['success'] = $success;
        return $result;
    }

    /**
     * 
     * @param string $table
     * @param array $migrations
     * @param mixed $currVersion
     * @param mixed $tempVersion
     * @param bool $allowErrors
     * @param array $result
     * @param string $output
     * @return bool
     */
    protected function runTableMigrations($table, $migrations, $currVersion, $tempVersion, $allowErrors, &$result, &$output)
    {
        $exec = 0;
        $done = 0;
        $fail = 0;
        $errors = 0;
        $returnValues = [];
        $versonChanged = false;
        $resultSuccess = true;
        $oldVersion = $currVersion;
        $recovered = $currVersion === false || $tempVersion !== null && (string)$tempVersion !== (string)$currVersion;
        $result['migrations'] = ['count' => count($migrations)];
        $mr = &$result['migrations'];

        ob_start();
        foreach ($migrations as $item) {
            $version = isset($item['version']) ? $item['version'] : null;
            $exception = null;
            if ($version !== null && $tempVersion !== null && $this->compareVersions($tempVersion, $version) > 0) {
                $version = null;
            }
            try {
                $exec++;
                $success = true;
                $return = $this->runMigration($item);
            } catch (\Exception $ex) {
                $success = false;
                $return = false;
                $exception = $ex;
            } catch (\Throwable $ex) {
                $success = false;
                $return = false;
                $exception = $ex;
            }
            if ($success) {
                $done++;
                $returnValues[] = $return;
                if ($return === false) {
                    $fail++;
                }
                if ($version !== null && (string)$tempVersion !== (string)$version) {
                    $currVersion = $tempVersion;
                    $tempVersion = $version;
                    $this->setTableVersion($table, false, $tempVersion);
                    $versonChanged = true;
                }
            } else {
                $errors++;
                $returnValues[] = false;
                $ignore = $allowErrors || !$versonChanged && $recovered;
                if (!$ignore) {
                    $result['exception'] = $exception;
                    $resultSuccess = false;
                    break;
                }
            }
            if ($oldVersion !== false && $oldVersion !== $currVersion) {
                $oldVersion = $currVersion;
                $this->setTableVersion($table, $currVersion, false);
            }
        }

        $output .= ob_get_clean();
        $mr['executed'] = $exec;
        $mr['done'] = $done;
        $mr['doneOk'] = $done - $fail;
        $mr['doneFailed'] = $fail;
        $mr['returnValues'] = $returnValues;
        $mr['errors'] = $errors;
        return $resultSuccess;
    }

    /**
     * 
     * @param TableUpgrader $tableUpgrader
     * @param array $result
     * @param string $output
     * @return bool
     */
    protected function runTableUpgrader($tableUpgrader, &$result, &$output)
    {
        $success = true;
        $done = 0;
        ob_start();
        try {
            echo "Running ".get_class($tableUpgrader)." on table '".$tableUpgrader->getRawTableName()."'\n";
            $commands = $tableUpgrader->getCommands();
            $result['upgrader']['count'] = count($commands);
            foreach ($commands as $command) {
                $command->execute();
                $done++;
            }
        } catch (\Exception $ex) {
            $result['exception'] = $ex;
            $success = false;
        } catch (\Throwable $ex) {
            $result['exception'] = $ex;
            $success = false;
        }
        $output .= ob_get_clean();
        $result['upgrader'] = ['done' => $done];
        return $success;
    }

    protected function runMigration($config)
    {
        $migration = $this->initMigration($config);
        if ($migration) {
            return $migration->up();
        }
        return false;
    }

    protected function buildMigrationConfig($config)
    {
        unset($config['since']);
        unset($config['featureId']);
        return $config;
    }

    protected function initMigration($config)
    {
        if ($config instanceof VersionedMigration) {
            return $config;
        }
        return new VersionedMigration($this->buildMigrationConfig($config));
    }

    public function createTableUpgrader($db, $table, $columns, $indexes = [], $options = [])
    {
        return TableUpgrader::create($db, $table, $columns, $indexes, $options);
    }

    public function versionIsGreater($version, $oldVersion)
    {
        if ($version === null) {
            return false;
        } elseif ($oldVersion === null) {
            return true;
        } else {
            $c = $this->compareVersions($version, $oldVersion);
            if ($c === 0) {
                return (string)$version !== (string)$oldVersion;
            }
            return $c >= 0;
        }
    }

    public function compareVersions($version1, $version2)
    {
        return (int)version_compare($version1, $version2);
    }

    protected function filterMigrationsMinVersion($migrations, $version)
    {
        $filtered = [];
        foreach ($migrations as $item) {
            if (isset($item['version'])) {
                $v = $item['version'];
                if ($this->compareVersions($v, $version) >= 0) {
                    $filtered[] = $item;
                }
            }
        }
        return $filtered;
    }

    /**
     * 
     * Only 'since' migrations ('since' !== false) are included if there is no old version.
     * If 'since' migrations has ID ('since' !== true), then:
     * - only newest is called if there is no old version;
     * - only oldest is called if there is old version.
     * 
     * @param array $migrations
     * @param mixed $newVersion
     * @param mixed $oldVersion
     */
    protected function extractMigrations($migrations, $newVersion, $oldVersion, $newerFirstDefault = true)
    {
        $versions = [];
        $prevVersion = null;
        $newerFirst = $newerFirstDefault;
        $olderFirst = !$newerFirst;
        $byVersion = [];
        $byFeatureId = [];
        foreach ($migrations as $item) {
            if (isset($item['version'])) {
                $version = $item['version'];
                $since = !empty($item['since']);
                $featureId = isset($item['featureId']) ? $item['featureId'] : null;
                $versions[] = $version;
                if ($oldVersion !== null || $since) {
                    $byVersion[(string)$version][] = $item;
                }
                if ($featureId !== null) {
                    $byFeatureId[$featureId][(string)$version][] = $item;
                }
                if ($prevVersion !== null) {
                    $c = $this->compareVersions($version, $prevVersion);
                    if ($c < 0) {
                        $olderFirst = false;
                    }
                    if ($c > 0) {
                        $newerFirst = false;
                    }
                }
                $prevVersion = $version;
            }
        }
        if (!$newerFirst && !$olderFirst) {
            $newerFirst = $newerFirstDefault;
            $olderFirst = !$newerFirst;
        }

        // sorting keeping initial order
        $uniqueVersions = $this->sortMigrationMaps($versions, $byVersion, $newerFirst);
        $featureLimits = $this->prepareFeatureLimits($uniqueVersions, $byFeatureId);
        return $this->extractMigrationsMapped($uniqueVersions, $byVersion, $featureLimits, $newVersion, $oldVersion);
    }

    private function sortMigrationMaps($versions, &$byVersion, $newerFirst)
    {
        $uniqueVersions = array_unique($versions, SORT_REGULAR);
        usort($uniqueVersions, [$this, 'compareVersions']);
        foreach ($byVersion as $key => $array) {
            if ($newerFirst) {
                $byVersion[$key] = array_reverse($array);
            }
        }
        return $uniqueVersions;
    }

    private function prepareFeatureLimits($versions, $byFeatureId)
    {
        $featureLimits = [];
        foreach ($byFeatureId as $id => $vmap) {
            $maxv = null;
            $minv = null;
            foreach ($versions as $version) {
                $v = (string)$version;
                if (!empty($vmap[$v])) {
                    $maxv = $version;
                    if ($minv === null) {
                        $minv = $version;
                    }
                }
            }
            $featureLimits[$id] = [$minv, $maxv];
        }
        return $featureLimits;
    }

    private function extractMigrationsMapped($versions, $byVersion, $featureLimits, $newVersion, $oldVersion)
    {
        $migrations = [];
        foreach ($versions as $version) {
            $cCurrent = $oldVersion !== null ? $this->compareVersions($version, $oldVersion) : 1;
            $cNew = $newVersion !== null ? $this->compareVersions($version, $newVersion) : 0;
            if ($cCurrent > 0 && $cNew <= 0) {
                $v = (string)$version;
                if (isset($byVersion[$v])) {
                    foreach ($byVersion[$v] as $item) {
                        $featureId = isset($item['featureId']) ? $item['featureId'] : null;
                        $add = true;
                        if ($featureId !== null) {
                            if (isset($featureLimits[$featureId])) {
                                list($minv, $maxv) = $featureLimits[$featureId];
                                if ($oldVersion === null) {
                                    $add = $version === $maxv;
                                } else {
                                    $add = $version === $minv;
                                }
                            }
                        }
                        if ($add) {
                            $migrations[] = $item;
                        }
                    }
                    unset($byVersion[$v]);
                }
            }
        }
        return $migrations;
    }

}
