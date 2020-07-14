<?php
/**
 * @package wr-elasticsearch
 * @author Erik Zigo <erik.zigo@keboola.com>
 */
namespace Keboola\ElasticsearchWriter\Options;

use Keboola\ElasticsearchWriter\Mapping\ColumnsMapper;

class LoadOptions
{
	const DEFAULT_BULK_SIZE = 10000;

	/** @var string */
	private $index;

	/** @var string */
	private $type;

	/** @var int */
	private $bulkSize = self::DEFAULT_BULK_SIZE;

	/** @var array */
	private $columns = [];


	public function setIndex($value): self
	{
		$this->index = (string) $value;
		return $this;
	}

	public function getIndex(): string
	{
		return $this->index;
	}

	public function setType($value): self
	{
		$this->type = (string) $value;
		return $this;
	}

	public function getType(): string
	{
		return $this->type;
	}

	public function setBulkSize($value): self
	{
		$this->bulkSize = (int) $value;
		return $this;
	}

	public function getBulkSize(): int
	{
		return $this->bulkSize;

	}

	public function setColumns(array $columns): self
	{
		$this->columns = $columns;
		return $this;
	}


	public function getColumns(): array
	{
		return $this->columns;
	}

	public function getColumnsMapper(): ColumnsMapper
	{
		return new ColumnsMapper($this->getColumns());
	}
}
