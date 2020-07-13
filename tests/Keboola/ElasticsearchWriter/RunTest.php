<?php
/**
 * @package wr-elasticsearch
 * @author Erik Zigo <erik.zigo@keboola.com>
 */
namespace Keboola\ElasticsearchWriter;

use Keboola\Csv\CsvFile;
use Keboola\ElasticsearchWriter\Exception\UserException;
use Symfony\Component\Yaml\Yaml;

class RunTest extends AbstractTest
{
	public function testUserError()
	{
		$lastOutput = exec('php ./src/run.php --data=./tests/data/run', $output, $returnCode);

		$this->assertEquals(1, $returnCode);
		$this->assertRegExp('/' . UserException::ERR_MISSING_CONFIG . '/ui', $lastOutput);

		unset($output);
		unset($lastOutput);
		unset($returnCode);

		$lastOutput = exec('php ./src/run.php 2>&1', $output, $returnCode);

		$this->assertEquals(1, $returnCode);
		$this->assertRegExp('/' . UserException::ERR_DATA_PARAM . '/ui', $lastOutput);

	}

	public function testRunAction()
	{
		$config = [
			"parameters" => [
				"elastic" => [
					"host" => getenv('EX_ES_HOST'),
					"port" => getenv('EX_ES_HOST_PORT'),
				],
				"tables" => [
					[
						"file" => "language-large.csv",
						"index" => $this->index,
						"type" => "language",
						"id" => "id",
						"export" => true,
					]
				]
			]
		];

		$yaml = Yaml::dump($config);

		$inTablesDir = './tests/data/run/in/tables';
		if (!is_dir($inTablesDir)) {
			mkdir($inTablesDir, 0777, true);
		}

		file_put_contents('./tests/data/run/config.yml', $yaml);

		copy('./tests/data/csv/language-large.csv', $inTablesDir . '/language-large.csv');

		$lastOutput = exec('php ./src/run.php --data=./tests/data/run', $output, $returnCode);

		$this->assertEquals(0, $returnCode);
		$this->assertRegExp('/Elasticsearch writer finished successfully/ui', $lastOutput);

		$this->assertEquals(1, $this->parseBatchCountFromOutput($output));
	}

	public function testRunWithBulkSizeAction()
	{
		$config = [
			"parameters" => [
				"elastic" => [
					"host" => getenv('EX_ES_HOST'),
					"port" => getenv('EX_ES_HOST_PORT'),
					"bulkSize" => 30
				],
				"tables" => [
					[
						"file" => "language-large.csv",
						"index" => $this->index,
						"type" => "language",
						"id" => "id",
						"export" => true,
					]
				]
			]
		];

		$yaml = Yaml::dump($config);

		$inTablesDir = './tests/data/run/in/tables';
		if (!is_dir($inTablesDir)) {
			mkdir($inTablesDir, 0777, true);
		}

		file_put_contents('./tests/data/run/config.yml', $yaml);

		copy('./tests/data/csv/language-large.csv', $inTablesDir . '/language-large.csv');

		$lastOutput = exec('php ./src/run.php --data=./tests/data/run', $output, $returnCode);

		$this->assertEquals(0, $returnCode);
		$this->assertRegExp('/Elasticsearch writer finished successfully/ui', $lastOutput);

		$expectedBatchCount = ceil($this->countTable(new CsvFile($inTablesDir . '/language-large.csv')) / 30);
		$this->assertEquals($expectedBatchCount, $this->parseBatchCountFromOutput($output));

	}

	public function testMappingAction()
	{
		// prepare data
		$config = [
			"parameters" => [
				"elastic" => [
					"host" => getenv('EX_ES_HOST'),
					"port" => getenv('EX_ES_HOST_PORT'),
					"bulkSize" => 10
				],
				"tables" => [
					[
						"file" => "language-large.csv",
						"index" => $this->index,
						"type" => "language",
						"id" => "id",
						"export" => true,
					]
				]
			]
		];

		$yaml = Yaml::dump($config);

		$inTablesDir = './tests/data/run/in/tables';
		if (!is_dir($inTablesDir)) {
			mkdir($inTablesDir, 0777, true);
		}

		file_put_contents('./tests/data/run/config.yml', $yaml);

		copy('./tests/data/csv/language-large.csv', $inTablesDir . '/language-large.csv');

		$lastOutput = exec('php ./src/run.php --data=./tests/data/run', $output, $returnCode);

		$this->assertEquals(0, $returnCode);
		$this->assertRegExp('/Elasticsearch writer finished successfully/ui', $lastOutput);

		$config = [
			"action" => "mapping",
			"parameters" => [
				"elastic" => [
					"host" => getenv('EX_ES_HOST'),
					"port" => getenv('EX_ES_HOST_PORT'),
					"bulkSize" => 10
				],
				"tables" => [
				]
			]
		];

		$yaml = Yaml::dump($config);

		file_put_contents('./tests/data/run/config.yml', $yaml);

		$lastOutput = exec('php ./src/run.php --data=./tests/data/run', $output, $returnCode);

		$this->assertEquals(0, $returnCode);

		$mapping = json_decode($lastOutput, true);
		$this->assertTrue(is_array($mapping));

		$this->assertArrayHasKey('indices', $mapping);

		// validate with client
		$writer = new Writer(sprintf('%s:%s', getenv('EX_ES_HOST'), getenv('EX_ES_HOST_PORT')));

		$indices = $writer->listIndices();
		$this->assertTrue(is_array($indices));

		$this->assertCount(count($indices), $mapping['indices']);

		$indiceFound = false;
		foreach ($indices AS $indice) {
			if ($indice['id'] === $this->index) {
				$indiceFound = true;
			}
		}

		$this->assertTrue($indiceFound);

		$indiceFound = false;
		foreach ($mapping['indices'] AS $indice) {
			$this->assertArrayHasKey('mappings', $indice);

			if ($indice['id'] === $this->index) {
				$indiceFound = true;

				$this->assertTrue(count($indice['mappings']) > 0);
			}
		}

		$this->assertTrue($indiceFound);
	}
}
