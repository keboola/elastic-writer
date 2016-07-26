<?php
/**
 * @package wr-elasticsearch
 * @author Erik Zigo <erik.zigo@keboola.com>
 */
namespace Keboola\ElasticsearchWriter;

use Elasticsearch;
use Keboola\ElasticsearchWriter\Exception\UserException;
use Symfony\Component\Yaml\Yaml;

class RunTest extends \PHPUnit_Framework_TestCase
{
	/**
	 * @var Writer
	 */
	protected $writer;

	protected $index = 'keboola_es_writer_run_test';

	protected function setUp()
	{
		$this->writer = new Writer(sprintf('%s:%s', getenv('EX_ES_HOST'), getenv('EX_ES_HOST_PORT')));

		$this->cleanUp();
	}

	/**
	 * Cleanup test workspace
	 *
	 * @throws Elasticsearch\Common\Exceptions\Missing404Exception
	 * @throws \Exception
	 */
	protected function cleanUp()
	{
		$params = ['index' => $this->index];

		if ($this->writer->getClient()->indices()->exists($params)) {
			$response = $this->writer->getClient()->indices()->delete($params);

			$this->assertArrayHasKey('acknowledged', $response);
			$this->assertTrue($response['acknowledged']);
		}

		$configPath = './tests/data/run/config.yml';
		if (file_exists($configPath)) {
			unlink($configPath);
		}
	}

	public function testUserError()
	{
		$lastOutput = exec('php ./src/run.php --data=./tests/data/run', $output, $returnCode);

		$this->assertEquals(1, $returnCode);
		$this->assertEquals(UserException::ERR_MISSING_CONFIG, $lastOutput);

		unset($output);
		unset($lastOutput);
		unset($returnCode);

		$lastOutput = exec('php ./src/run.php 2>&1', $output, $returnCode);

		$this->assertEquals(1, $returnCode);
		$this->assertEquals(UserException::ERR_DATA_PARAM, $lastOutput);
	}

	public function testRunAction()
	{
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
		$this->assertEquals('Elasticsearch writer finished successfully.', $lastOutput);
		$this->assertCount(6, $output);
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
		$this->assertEquals('Elasticsearch writer finished successfully.', $lastOutput);
		$this->assertCount(6, $output);

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