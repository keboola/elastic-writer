<?php

declare(strict_types=1);

namespace Keboola\ElasticsearchWriter\Tests;

use Keboola\Component\Logger;
use Keboola\Csv\CsvReader;
use Keboola\ElasticsearchWriter\Writer;

class RunTest extends AbstractTestClass
{

    public function testRunAction(): void
    {
        $config = [
            'parameters' => [
                'elastic' => [
                    'host' => getenv('EX_ES_HOST'),
                    'port' => getenv('EX_ES_HOST_PORT'),
                ],
                'tables' => [
                    [
                        'file' => 'language-large.csv',
                        'index' => $this->index,
                        'type' => 'language',
                        'id' => 'id',
                        'export' => true,
                    ],
                ],
            ],
        ];

        file_put_contents($this->dataDir->getTmpFolder() . '/config.json', json_encode($config));

        copy(
            __DIR__ . '/../data/csv/language-large.csv',
            $this->dataDir->getTmpFolder() . '/in/files/language-large.csv',
        );

        $process = $this->runProcess();

        $this->assertEquals(0, $process->getExitCode());
        $this->assertMatchesRegularExpression('/File language-large.csv - Export finished/ui', $process->getOutput());

        $this->assertEquals(1, $this->parseBatchCountFromOutput($process->getOutput()));
    }

    public function testRunWithBulkSizeAction(): void
    {
        $config = [
            'parameters' => [
                'elastic' => [
                    'host' => getenv('EX_ES_HOST'),
                    'port' => getenv('EX_ES_HOST_PORT'),
                    'bulkSize' => 30,
                ],
                'tables' => [
                    [
                        'file' => 'language-large.csv',
                        'index' => $this->index,
                        'type' => 'language',
                        'id' => 'id',
                        'export' => true,
                    ],
                ],
            ],
        ];

        file_put_contents($this->dataDir->getTmpFolder() . '/config.json', json_encode($config));

        copy(
            __DIR__ . '/../data/csv/language-large.csv',
            $this->dataDir->getTmpFolder() . '/in/files/language-large.csv',
        );

        $process = $this->runProcess();

        $this->assertEquals(0, $process->getExitCode());
        $this->assertMatchesRegularExpression('/File language-large.csv - Export finished/ui', $process->getOutput());

        $expectedBatchCount = ceil($this->countTable(new CsvReader(__DIR__ . '/../data/csv/language-large.csv')) / 30);
        $this->assertEquals($expectedBatchCount, $this->parseBatchCountFromOutput($process->getOutput()));
    }

    public function testMappingAction(): void
    {
        // prepare data
        $config = [
            'parameters' => [
                'elastic' => [
                    'host' => getenv('EX_ES_HOST'),
                    'port' => getenv('EX_ES_HOST_PORT'),
                    'bulkSize' => 10,
                ],
                'tables' => [
                    [
                        'file' => 'language-large.csv',
                        'index' => $this->index,
                        'type' => 'language',
                        'id' => 'id',
                        'export' => true,
                    ],
                ],
            ],
        ];

        file_put_contents($this->dataDir->getTmpFolder() . '/config.json', json_encode($config));

        copy(
            __DIR__ . '/../data/csv/language-large.csv',
            $this->dataDir->getTmpFolder() . '/in/files/language-large.csv',
        );

        $process = $this->runProcess();

        $this->assertEquals(0, $process->getExitCode());
        $this->assertMatchesRegularExpression('/File language-large.csv - Export finished/ui', $process->getOutput());

        $config = [
            'action' => 'mapping',
            'parameters' => [
                'elastic' => [
                    'host' => getenv('EX_ES_HOST'),
                    'port' => getenv('EX_ES_HOST_PORT'),
                    'bulkSize' => 10,
                ],
                'tables' => [],
            ],
        ];

        file_put_contents($this->dataDir->getTmpFolder() . '/config.json', json_encode($config));

        copy(
            __DIR__ . '/../data/csv/language-large.csv',
            $this->dataDir->getTmpFolder() . '/in/files/language-large.csv',
        );

        $process = $this->runProcess();

        $this->assertEquals(0, $process->getExitCode());

        $mapping = json_decode($process->getOutput(), true);
        $this->assertTrue(is_array($mapping));

        $this->assertArrayHasKey('indices', $mapping);

        // validate with client
        $writer = new Writer(
            sprintf('%s:%s', getenv('EX_ES_HOST'), getenv('EX_ES_HOST_PORT')),
            new Logger(),
        );

        $indices = $writer->listIndices();
        $this->assertTrue(is_array($indices));

        $this->assertCount(count($indices), $mapping['indices']);

        $indiceFound = false;
        foreach ($indices as $indice) {
            if ($indice['id'] === $this->index) {
                $indiceFound = true;
            }
        }

        $this->assertTrue($indiceFound);

        $indiceFound = false;
        foreach ($mapping['indices'] as $indice) {
            $this->assertArrayHasKey('mappings', $indice);

            if ($indice['id'] === $this->index) {
                $indiceFound = true;

                $this->assertTrue(count($indice['mappings']) > 0);
            }
        }

        $this->assertTrue($indiceFound);
    }

    public function testRunWithListAction(): void
    {
        $config = [
            'parameters' => [
                'elastic' => [
                    'host' => getenv('EX_ES_HOST'),
                    'port' => getenv('EX_ES_HOST_PORT'),
                ],
                'tables' => [
                    [
                        'file' => 'language-with-list.csv',
                        'index' => $this->index,
                        'type' => 'language-with-list',
                        'id' => 'id',
                        'export' => true,
                        'items' => [
                            [
                                'name' => 'id',
                                'dbName' => 'id',
                                'type' => 'integer',
                                'nullable' => false,
                            ],
                            [
                                'name' => 'list',
                                'dbName' => 'list',
                                'type' => 'array',
                                'nullable' => true,
                            ],
                        ],
                    ],
                ],
            ],
        ];

        file_put_contents($this->dataDir->getTmpFolder() . '/config.json', json_encode($config));

        copy(
            __DIR__ . '/../data/csv/language-with-list.csv',
            $this->dataDir->getTmpFolder() . '/in/files/language-with-list.csv',
        );

        $process = $this->runProcess();

        $this->assertEquals(0, $process->getExitCode());
        $this->assertMatchesRegularExpression(
            '/File language-with-list.csv - Export finished/ui',
            $process->getOutput(),
        );
        $this->assertEquals(1, $this->parseBatchCountFromOutput($process->getOutput()));

        $writer = $this->writer;
        $writer->getClient()->indices()->refresh(['index' => $this->index]);

        $params = [
            'index' => $this->index,
            'type' => 'language-with-list',
        ];

        $items = $writer->getClient()->search($params);

        $this->assertCount(4, $items);

        $item24 = $writer->getClient()->get([
            'index' => $this->index,
            'type' => 'language-with-list',
            'id' => 24,
        ]);

        $this->assertEquals([
            'id' => 24,
            'name' => 'french',
            'iso' => 'fr',
            'something' => '',
            'list' => [5],
        ], $item24['_source']);

        $item26 = $writer->getClient()->get([
            'index' => $this->index,
            'type' => 'language-with-list',
            'id' => 26,
        ]);

        $this->assertEquals([
            'id' => 26,
            'name' => 'czech',
            'iso' => 'cs',
            'something' => '',
            'list' => null,
        ], $item26['_source']);
    }
}
