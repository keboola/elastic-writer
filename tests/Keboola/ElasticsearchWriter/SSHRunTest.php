<?php
/**
 * @package wr-elasticsearch
 * @author Erik Zigo <erik.zigo@keboola.com>
 */
namespace Keboola\ElasticsearchWriter;

use Elasticsearch;
use Keboola\ElasticsearchWriter\Exception\UserException;
use Symfony\Component\Yaml\Yaml;

class SSHRunTest extends \PHPUnit_Framework_TestCase
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

	public function testRunAction()
	{
		$config = [
			"parameters" => [
				"elastic" => [
					"host" => getenv('EX_ES_HOST'),
					"port" => getenv('EX_ES_HOST_PORT'),
					"bulkSize" => 10,
					"ssh" => [
						"enabled" => true,
						"user" => "root",
						"sshHost" => "sshproxy",
						"keys" => [
							'private' => getenv('EX_ES_SSH_KEY_PRIVATE'),
						]
					]
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

		$tunnelCreated = false;
		foreach ($output AS $line) {
			if (preg_match('/Creating SSH tunnel to/ui', $line)) {
				$tunnelCreated = true;
			}
		}

		$this->assertTrue($tunnelCreated);

		$this->assertEquals(0, $returnCode);
		$this->assertRegExp('/Elasticsearch writer finished successfully/ui', $lastOutput);
		$this->assertCount(7, $output);
	}

	public function testMappingAction()
	{
		// prepare data
		$config = [
			"parameters" => [
				"elastic" => [
					"host" => getenv('EX_ES_HOST'),
					"port" => getenv('EX_ES_HOST_PORT'),
					"bulkSize" => 10,
					"ssh" => [
						"enabled" => true,
						"user" => "root",
						"sshHost" => "sshproxy",
						"localPort" => "29200",
						"keys" => [
							'private' => getenv('EX_ES_SSH_KEY_PRIVATE'),
						]
					]
				],
				"ssh" => [
					"enabled" => true,
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

		$tunnelCreated = false;
		foreach ($output AS $line) {
			if (preg_match('/Creating SSH tunnel to/ui', $line)) {
				$tunnelCreated = true;
			}
		}

		$this->assertTrue($tunnelCreated);

		$this->assertEquals(0, $returnCode);
		$this->assertRegExp('/Elasticsearch writer finished successfully/ui', $lastOutput);
		$this->assertCount(7, $output);

		$config = [
			"action" => "mapping",
			"parameters" => [
				"elastic" => [
					"host" => getenv('EX_ES_HOST'),
					"port" => getenv('EX_ES_HOST_PORT'),
					"bulkSize" => 10,
					"ssh" => [
						"enabled" => true,
						"user" => "root",
						"sshHost" => "sshproxy",
						"localPort" => "39200",
						"keys" => [
							'private' => getenv('EX_ES_SSH_KEY_PRIVATE'),
						]
					]
				],
				"ssh" => [
					"enabled" => true,
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