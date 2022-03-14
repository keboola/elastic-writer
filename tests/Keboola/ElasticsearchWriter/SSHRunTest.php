<?php
/**
 * @package wr-elasticsearch
 * @author Erik Zigo <erik.zigo@keboola.com>
 */
namespace Keboola\ElasticsearchWriter;

use Symfony\Component\Yaml\Yaml;

class SSHRunTest extends AbstractTest
{
	protected function cleanUp()
	{
		parent::cleanUp();

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
	}

    public function testRunActionWithWrongPort()
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
                        'sshPort' => "1234", //invalid port
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

        $this->assertRegExp('/Unable to create SSH tunnel/ui', $lastOutput);
        $this->assertEquals(1, $returnCode);

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
