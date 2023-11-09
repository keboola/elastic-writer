<?php

declare(strict_types=1);

namespace Keboola\ElasticsearchWriter\Mapping;

use Exception;
use Generator;
use Keboola\ElasticsearchWriter\Exception\UserException;
use Nette\Utils\Json;
use Throwable;

class ColumnsMapper
{
    public const INTEGER_TYPES = ['long', 'integer', 'short', 'byte'];

    public const FLOAT_TYPES = ['double', 'float', 'half_float', 'scaled_float'];

    public const BOOLEAN_TYPES = ['boolean'];

    public const ARRAY_TYPES = ['array'];

    public const IGNORED_COLUMN_TYPE = 'ignore';

    /** @var ColumnMapping[] */
    private array $columnsByCsvName = [];

    public function __construct(array $columns)
    {
        foreach ($columns as $data) {
            $column = new ColumnMapping($data);
            $this->columnsByCsvName[$column->getCsvName()] = $column;
        }
    }

    public function getAllColumns(): array
    {
        return $this->columnsByCsvName;
    }

    public function mapCsvRow(array $csvHeader, array $values): Generator
    {
        foreach ($csvHeader as $index => $csvName) {
            if (!isset($values[$index])) {
                throw new UserException('Invalid CSV file.');
            }

            $column = $this->columnsByCsvName[$csvName] ?? null;
            if (!$column) {
                // Column is not present in mapping, it is used without change
                yield $csvName => $values[$index];
            } elseif ($column->getType() !== self::IGNORED_COLUMN_TYPE) {
                yield $column->getDbName() => $this->mapValue($column, (string) $values[$index]);
            }
        }
    }

    /**
     * @return mixed
     */
    private function mapValue(ColumnMapping $column, string $value)
    {
        if ($value === '' && $column->isNullable()) {
            return null;
        }

        $type = $column->getType();
        if (in_array($type, self::INTEGER_TYPES, true)) {
            $intVal = (int) $value;
            // PHP int is 32 bit, so for longer integers return string
            return (string) $intVal === $value ? $intVal : $value;
        }

        if (in_array($type, self::FLOAT_TYPES, true)) {
            return (float) $value;
        }

        if (in_array($type, self::BOOLEAN_TYPES, true)) {
            $value = strtolower($value);
            $firstLetter = substr($value, 0, 1);
            if ($firstLetter === 't') {
                // "t", or "true"
                return true;
            } elseif ($firstLetter === 'f') {
                // "f", or "false"
                return false;
            }
            return (bool) $value;
        }

        if (in_array($type, self::ARRAY_TYPES, true)) {
            try {
                return Json::decode($value, true);
            } catch (Throwable) {
                throw new UserException(
                    'Could not decode value of type array. Value: ' .
                    var_export($value, true),
                );
            }
        }

        return $value;
    }
}
