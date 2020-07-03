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
		$this->csvName = $data['name'];
		$this->dbName = $data['dbName'];
		$this->type = $data['type'];
		$this->nullable = $data['nullable'];
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
