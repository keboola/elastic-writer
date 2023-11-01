<?php

declare(strict_types=1);

namespace Keboola\ElasticsearchWriter\Tests;

use Keboola\Component\Logger;
use Keboola\ElasticsearchWriter\Writer;
use PHPUnit\Framework\Assert;
use Symfony\Component\Process\Process;

class SSHRunTest extends AbstractTestClass
{
	protected function cleanUp(): void
	{
		parent::cleanUp();

		$configPath = __DIR__ . '/../data/run/config.json';
		if (file_exists($configPath)) {
			unlink($configPath);
		}
	}

	public function testRunAction(): void
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
							'private' => (string) file_get_contents('/root/.ssh/id_rsa'),
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

        file_put_contents($this->dataDir->getTmpFolder() . '/config.json', json_encode($config));

		copy(
            __DIR__ . '/../data/csv/language-large.csv',
            $this->dataDir->getTmpFolder() . '/in/files/language-large.csv'
        );

        $process = $this->runProcess();

        Assert::assertMatchesRegularExpression('/Creating SSH tunnel to/ui', $process->getOutput());

		$this->assertEquals(0, $process->getExitCode());
		$this->assertMatchesRegularExpression(
            '/File language-large.csv - Export finished/ui',
            $process->getOutput()
        );
	}

    public function testRunActionWithWrongPort(): void
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
                            'private' => (string) file_get_contents('/root/.ssh/id_rsa'),
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

        file_put_contents($this->dataDir->getTmpFolder() . '/config.json', json_encode($config));

        copy(
            __DIR__ . '/../data/csv/language-large.csv',
            $this->dataDir->getTmpFolder() . '/in/files/language-large.csv'
        );

        $process = $this->runProcess();

        $this->assertMatchesRegularExpression('/Unable to create SSH tunnel/ui', $process->getErrorOutput());
        $this->assertEquals(1, $process->getExitCode());

    }

	public function testMappingAction(): void
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
                            'private' => (string) file_get_contents('/root/.ssh/id_rsa'),
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

        file_put_contents($this->dataDir->getTmpFolder() . '/config.json', json_encode($config));

        copy(
            __DIR__ . '/../data/csv/language-large.csv',
            $this->dataDir->getTmpFolder() . '/in/files/language-large.csv'
        );

        $process = $this->runProcess();

        $this->assertMatchesRegularExpression('/Creating SSH tunnel to/ui', $process->getOutput());

		$this->assertEquals(0, $process->getExitCode());
		$this->assertMatchesRegularExpression('/File language-large.csv - Export finished/ui', $process->getOutput());

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
                            'private' => (string) file_get_contents('/root/.ssh/id_rsa'),
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
        file_put_contents($this->dataDir->getTmpFolder() . '/config.json', json_encode($config));

        copy(
            __DIR__ . '/../data/csv/language-large.csv',
            $this->dataDir->getTmpFolder() . '/in/files/language-large.csv'
        );

        $process = $this->runProcess();

		$this->assertEquals(0, $process->getExitCode());

		$mapping = json_decode($process->getOutput(), true);
		$this->assertTrue(is_array($mapping));

		$this->assertArrayHasKey('indices', $mapping);

		// validate with client
		$writer = new Writer(
            sprintf('%s:%s', getenv('EX_ES_HOST'), getenv('EX_ES_HOST_PORT')),
            new Logger()
        );

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
