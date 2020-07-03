<?php

namespace Keboola\ElasticsearchWriter\Mapping;


class ColumnMapping
{
	/** @var string */
	private $csvName;

	/** @var string */
	private $dbName;

	/** @var string */
	private $type;

	/** @var bool */
	private $nullable;

	public function __construct(array $data)
	{
		$this->csvName = $data[''];
		$this->dbName = $data[''];
		$this->type = $data[''];
		$this->nullable = $data[''];
	}

	public function getCsvName(): string
	{
		return $this->csvName;
	}

	public function getDbName(): string
	{
		return $this->dbName;
	}

	public function getType(): string
	{
		return $this->type;
	}

	public function isNullable(): bool
	{
		return $this->nullable;
	}
}
