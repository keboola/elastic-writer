<?php

namespace Keboola\ElasticsearchWriter\Mapping;

use Generator;
use Keboola\ElasticsearchWriter\Exception\UserException;
use Nette\Utils\Json;

class ColumnsMapper
{
	const INTEGER_TYPES = ['long', 'integer', 'short', 'byte'];

	const FLOAT_TYPES = ['double', 'float', 'half_float', 'scaled_float'];

	const BOOLEAN_TYPES = ['boolean'];

	const ARRAY_TYPES = ['array'];

	const IGNORED_COLUMN_TYPE = 'ignore';

	/** @var ColumnMapping[] */
	private $columnsByCsvName = [];

	public function __construct(array $columns)
	{
		foreach ($columns as $data) {
			$column = new ColumnMapping($data);
			$this->columnsByCsvName[$column->getCsvName()] = $column;
		}
	}

	public function getAllColumns()
	{
		return $this->columnsByCsvName;
	}

	public function mapCsvRow(array &$csvHeader, array &$values): Generator
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
			$firstLetter = substr($value, 0, 1) ;
			if ($firstLetter === 't') {
				// "t", or "true"
				return true;
			} else if ($firstLetter === 'f') {
				// "f", or "false"
				return false;
			}
			return (bool) $value;
		}

		if (in_array($type, self::ARRAY_TYPES, true)) {
			try {
				return Json::decode($value, Json::FORCE_ARRAY);
			} catch (\Exception $e) {
				throw new UserException('Could not decode value of type array. Value: ' . var_export($value, true));
			}
		}

		return $value;
	}
}
