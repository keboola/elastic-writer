<?php
/**
 * @package wr-elasticsearch
 * @author Erik Zigo <erik.zigo@keboola.com>
 */
namespace Keboola\ElasticsearchWriter;

use Elasticsearch;
use Keboola\Csv\CsvFile;
use Symfony\Component\Process\Process;

abstract class AbstractTest extends \PHPUnit_Framework_TestCase
{
	protected function tearDown()
	{
		parent::tearDown();

		# Close SSH tunnel if created
		$process = new Process(['sh', '-c', 'pgrep ssh | xargs -r kill']);
		$process->mustRun();
	}

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
