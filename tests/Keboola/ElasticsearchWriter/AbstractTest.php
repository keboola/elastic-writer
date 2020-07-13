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
	/** @var Writer */
	protected $writer;

	protected $index = 'keboola_es_writer_test';

	protected function setUp()
	{
		$this->writer = new Writer(sprintf('%s:%s', getenv('EX_ES_HOST'), getenv('EX_ES_HOST_PORT')));
		$this->cleanUp();
	}

	protected function tearDown()
	{
		parent::tearDown();
		$this->cleanUp();

		# Close SSH tunnel if created
		$process = new Process(['sh', '-c', 'pgrep ssh | xargs -r kill']);
		$process->mustRun();
	}

	/**
	 * Cleanup test workspace
	 *
	 * @throws Elasticsearch\Common\Exceptions\Missing404Exception
	 * @throws \Exception
	 */
	protected function cleanUp()
	{
		// Remove all indexes
		foreach ($this->writer->getClient()->cat()->indices() as $data) {
			$response = $this->writer->getClient()->indices()->delete([
				'index' => $data['index']
			]);

			$this->assertArrayHasKey('acknowledged', $response);
			$this->assertTrue($response['acknowledged']);
		}


		$configPath = './tests/data/run/config.yml';
		if (file_exists($configPath)) {
			unlink($configPath);
		}
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
