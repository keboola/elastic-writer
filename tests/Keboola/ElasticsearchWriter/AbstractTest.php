<?php
/**
 * @package wr-elasticsearch
 * @author Erik Zigo <erik.zigo@keboola.com>
 */
namespace Keboola\ElasticsearchWriter;

use Elasticsearch;
use Keboola\Csv\CsvFile;

abstract class AbstractTest extends \PHPUnit_Framework_TestCase
{
	/**
	 * Count records in CSV (with headers)
	 *
	 * @param CsvFile $file
	 * @return int
	 */
	protected function countTable(CsvFile $file)
	{
		$linesCount = 0;
		foreach ($file AS $i => $line)
		{
			// skip header
			if (!$i) {
				continue;
			}

			$linesCount++;
		}

		return $linesCount;
	}

	/**
	 * @param array $output
	 * @return int
	 */
	protected function parseBatchCountFromOutput(array $output)
	{
		$count = 0;
		foreach ($output as $line) {
			if (preg_match('/Write .+ batch [0-9]+ to .+ start/ui', $line)) {
				$count++;
			}
		}

		return $count;
	}
}