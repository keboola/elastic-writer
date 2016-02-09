<?php
/**
 * @package wr-elasticsearch
 * @author Erik Zigo <erik.zigo@keboola.com>
 */
namespace Keboola\ElasticsearchWriter\Options;

class LoadOptions
{
	const DEFAULT_BULK_SIZE = 10000;

	/**
	 * @var string
	 */
	private $index;

	/**
	 * @var string
	 */
	private $type;

	/**
	 * @var int
	 */
	private $bulkSize = self::DEFAULT_BULK_SIZE;

	/**
	 * @param $value
	 * @return $this
	 */
	public function setIndex($value)
	{
		$this->index = (string) $value;
		return $this;
	}

	/**
	 * @return string
	 */
	public function getIndex()
	{
		return $this->index;
	}

	/**
	 * @param $value
	 * @return $this
	 */
	public function setType($value)
	{
		$this->type = (string) $value;
		return $this;
	}

	/**
	 * @return string
	 */
	public function getType()
	{
		return $this->type;
	}

	/**
	 * @param $value
	 * @return $this
	 */
	public function setBulkSize($value)
	{
		$this->bulkSize = (int) $value;
		return $this;
	}

	/**
	 * @return int
	 */
	public function getBulkSize()
	{
		return $this->bulkSize;

	}
}