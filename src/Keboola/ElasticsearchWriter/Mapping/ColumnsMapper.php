<?php

namespace Keboola\ElasticsearchWriter\Mapping;

use Generator;

class ColumnsMapper
{
	/** @var ColumnMapping[] */
	private $columnsByCsvName = [];

	public function __construct(array $columns)
	{
		foreach ($columns as $data) {
			$column = new ColumnMapping($data);
			$this->columnsByCsvName[$column->getCsvName()] = $column;
		}
	}

	public function mapCsvRow(array &$csvHeader, array &$values): Generator
	{
		foreach ($csvHeader as $index => $csvName) {
			$column = $this->columnsByCsvName[$csvName] ?? null;
			if (!$column) {
				// Column is not present in mapping, it is used without change
				yield $csvName => $values[$index];
			} elseif ($column->getType() !== 'IGNORE') {
				yield $column->getDbName() => $this->mapValue($column, (string) $values[$index]);
			}
		}
	}

	private function mapValue(ColumnMapping $column, string $value)
	{
		// TODO
		return $value;
	}
}
