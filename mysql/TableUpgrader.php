<?php

/*
 * Copyright (c) 2021 Rick
 *
 * This file is a part of a project 'megabike'.
 * MIT licence.
 */

namespace megabike\yii2\migration\mysql;

use megabike\yii2\migration\TableUpgrader as AbstractTableUpgrader;

/**
 *
 * @author Rick
 */
class TableUpgrader extends AbstractTableUpgrader
{

    protected function getStoredColumnsInfo()
    {
        $columns = array();
        $command = $this->db->createCommand("SHOW FULL FIELDS FROM ".$this->db->quoteTableName($this->table));
        foreach ($command->query() as $row) {
            $columns[$row['Field']] = $row;
        }
        return $columns;
    }

    protected function getStoredIndexesInfo()
    {
        $indexes = array();
        $command = $this->db->createCommand("SHOW INDEX FROM ".$this->db->quoteTableName($this->table));
        foreach ($command->query() as $row) {
            $indexes[$row['Key_name']][] = $row;
        }
        return $indexes;
    }

    protected function getStoredOptionsInfo()
    {
        $name = $this->db->getSchema()->getRawTableName($this->table);
        $command = $this->db->createCommand("SHOW TABLE STATUS WHERE Name=".$this->db->quoteValue($name));
        return $command->queryOne();
    }

    protected function getStoredIndexAttrs($storedIndex)
    {
        if (isset($storedIndex[0])) {
            $attrs = [];
            $attrs['name'] = $storedIndex[0]['Key_name'];
            $attrs['primary'] = $storedIndex[0]['Key_name'] === 'PRIMARY';
            $attrs['unique'] = (int)$storedIndex[0]['Non_unique'] === 0;
            $columns = [];
            foreach ($storedIndex as $item) {
                $columns[] = $item['Column_name'];
            }
            $attrs['columns'] = $columns;
            return $attrs;
        }
        parent::getStoredIndexAttrs($storedIndex);
    }

    protected function getCorrespondingIndexKey($name, $definition, $storedIndexes = null)
    {
        if (!empty($definition['primary'])) {
            return 'PRIMARY';
        }
        return $name;
    }

    protected function checkIndexNeedsUpgrade($newIndex, $oldIndex)
    {
        $columns = $newIndex['columns'];
        $oldColumns = [];
        foreach ($oldIndex as $item) {
            $oldColumns[] = $item['Column_name'];
        }
        if (count($columns) !== count($oldColumns)) {
            return true;
        }
        foreach ($columns as $i => $c) {
            if ((string)$c !== (string)$oldColumns[$i]) {
                return true;
            }
        }

        $name = $newIndex['name'];
        $oldName = $oldIndex[0]['Key_name'];
        if ((string)$name !== (string)$oldName && $oldName !== 'PRIMARY') {
            return true;
        }

        $isPrimary = !empty($newIndex['primary']);
        $oldIsPrimary = $oldIndex[0]['Key_name'] === 'PRIMARY';
        if ($isPrimary !== $oldIsPrimary) {
            return true;
        }

        $isUnique = !empty($newIndex['unique']) || $isPrimary;
        $oldIsUnique = (int)$oldIndex[0]['Non_unique'] === 0;
        if ($isUnique !== $oldIsUnique) {
            return true;
        }

        return false;
    }

    protected function escapeSpecial($string)
    {
        return strtolower(preg_replace('/\W/', '', $string));
    }

    protected function compareCollation($collationValue, $charset, $collation = null)
    {
        $charsetId = $this->normalizeCharset($charset);
        if ($collation === null) {
            $prefix = $charsetId.'_';
            return !strncasecmp($prefix, $collationValue, strlen($prefix));
        } else {
            list(, $collation) = $this->applyCharsetDefaults($charset, $collation);
            return !strcasecmp($collationValue, $collation);
        }
    }

    protected function normalizeCharset($charset)
    {
        $charsetId = parent::normalizeCharset($charset);
        return $charsetId !== null ? $this->escapeSpecial($charsetId) : null;
    }

    protected function getBinaryCollation($charsetId)
    {
        return $charsetId !== null ? $charsetId.'_bin' : null;
    }

    protected function getPreferredCollation($charsetId)
    {
        if ($charsetId === null) {
            return null;
        }
        if ($charsetId === 'utf8' || $charsetId === 'utf8mb4' || $charsetId === 'utf16') {
            return $charsetId.'_unicode_ci';
        }
        return $charsetId.'_general_ci';
    }

    protected function buildColumnCharsetDefinition($charset, $collation = null)
    {
        $sql = '';
        if ($charset !== null) {
            $sql .= " CHARACTER SET ".$this->normalizeCharset($charset);
        }
        if ($collation !== null) {
            $sql .= " COLLATE ".$this->escapeSpecial($collation);
        }
        return $sql ? ltrim($sql) : null;
    }

    protected function checkColumnNeedsUpgrade($newColumn, $schemaColumn, $oldColumn)
    {
        if (parent::checkColumnNeedsUpgrade($newColumn, $schemaColumn, $oldColumn)) {
            return true;
        }
        if (isset($oldColumn['Collation'])) {
            list($charset, $collation) = $this->getColumnCharsetAndCollation($newColumn);
            if (($charset !== null || $collation !== null) && !$this->compareCollation($oldColumn['Collation'], $charset, $collation)) {
                return true;
            }
        }
        return false;
    }

    protected function buildOptions($options)
    {
        $items = [];

        $charset = isset($options['charset']) ? $options['charset'] : $this->getDefaultCharset();
        $collation = isset($options['collation']) ? $options['collation'] : null;
        if ($charset !== null) {
            list($charset, $collation) = $this->applyCharsetDefaults($charset, $collation);
            $items[] = "DEFAULT CHARACTER SET ".$this->normalizeCharset($charset);
            if ($collation !== null) {
                $items[] = "COLLATE ".$this->escapeSpecial($collation);
            }
        }

        if (isset($options['engine'])) {
            $items[] = "ENGINE=".$this->escapeSpecial($options['engine']);
        }

        if (isset($options['comment'])) {
            $items[] = "COMMENT=".$this->db->quoteValue((string)$options['comment']);
        }

        return $items;
    }

    protected function getAlterTableCommand($optionsString)
    {
        if ((string)$optionsString !== '') {
            return $this->db->createCommand("ALTER TABLE ".$this->db->quoteTableName($this->table)." ".$optionsString);
        }
        return null;
    }

    protected function checkOptionsNeedUpgrade($newOptions, $oldOptions)
    {
        if (parent::checkOptionsNeedUpgrade($newOptions, $oldOptions)) {
            return true;
        }
        if (isset($oldOptions['Engine']) && isset($newOptions['engine']) && strcasecmp((string)$oldOptions['Engine'], (string)$newOptions['engine'])) {
            return true;
        }
        if (isset($oldOptions['Comment']) && isset($newOptions['comment']) && (string)$oldOptions['Comment'] !== (string)$newOptions['comment']) {
            return true;
        }

        $charset = isset($newOptions['charset']) ? $newOptions['charset'] : $this->getDefaultCharset();
        if ($charset !== null && isset($oldOptions['Collation'])) {
            $collation = isset($newOptions['collation']) ? $newOptions['collation'] : null;
            if (!$this->compareCollation($oldOptions['Collation'], $charset, $collation)) {
                return true;
            }
        }

        return false;
    }

    protected function getColumnUpgradeDefinitionString($definition, $after = null)
    {
        $string = parent::getColumnUpgradeDefinitionString($definition, $after);
        if ($definition->isPrimaryKey() && $this->tableSchema && isset($this->tableSchema->columns[$definition->getName()])) {
            $column = $this->tableSchema->columns[$definition->getName()];
            if ($column->isPrimaryKey) {
                $string = preg_replace('/\s+PRIMARY\s+KEY\s*/i', ' ', $string);
            }
        }
        return $string;
    }

}
