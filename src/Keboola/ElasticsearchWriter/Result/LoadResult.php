<?php
/**
 * @package wr-elasticsearch
 * @author Erik Zigo <erik.zigo@keboola.com>
 */
namespace Keboola\ElasticsearchWriter\Result;

class LoadResult
{
	private $docCount = 0;
	private $bulkCount = 0;

	public function __construct($docCount, $bulkCount)
	{
		$this->docCount = (int) $docCount;
		$this->bulkCount = (int) $bulkCount;
	}

	/**
	 * @return int
	 */
	public function getDocCount()
	{
		return $this->docCount;
	}

	/**
	 * @return int
	 */
	public function getBulkCount()
	{
		return $this->bulkCount;
	}

}