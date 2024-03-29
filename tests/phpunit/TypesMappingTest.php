<?php

declare(strict_types=1);

namespace Keboola\ElasticsearchWriter\Tests;

use PHPUnit\Framework\Assert;
use Symfony\Component\Process\Process;

class TypesMappingTest extends AbstractTestClass
{
    private bool $textSupportedBySrv;

    protected function setUp(): void
    {
        parent::setUp();

        // https://www.elastic.co/blog/strings-are-dead-long-live-strings
        $version = $this->writer->getClient()->info()['version']['number'];
        $this->textSupportedBySrv = version_compare($version, '5.0', '>=');
    }

    public function testRunActionWithColumnsMapping(): void
    {
        $stringType = $this->textSupportedBySrv ? 'text' : 'string';
        $config = [
            'parameters' => [
                'elastic' => [
                    'host' => getenv('EX_ES_HOST'),
                    'port' => getenv('EX_ES_HOST_PORT'),
                ],
                'tables' => [
                    [
                        'file' => 'types.csv',
                        'index' => $this->index,
                        'type' => 'types',
                        'id' => 'long-db',
                        'export' => true,
                        'items' => [
                            [
                                'name' => 'long',
                                'dbName' => 'long-db',
                                'type' => 'long',
                                'nullable' => false,
                            ],
                            [
                                'name' => 'int-nullable',
                                'dbName' => 'int-nullable',
                                'type' => 'integer',
                                'nullable' => true,
                            ],
                            [
                                'name' => 'double',
                                'dbName' => 'double-db',
                                'type' => 'double',
                                'nullable' => false,
                            ],
                            [
                                'name' => 'float-nullable',
                                'dbName' => 'float-nullable',
                                'type' => 'float',
                                'nullable' => true,
                            ],
                            [
                                'name' => 'bool',
                                'dbName' => 'bool-db',
                                'type' => 'boolean',
                                'nullable' => false,
                            ],
                            [
                                'name' => 'bool-nullable',
                                'dbName' => 'bool-nullable',
                                'type' => 'boolean',
                                'nullable' => true,
                            ],
                            [
                                'name' => 'ignored',
                                'dbName' => 'ignored',
                                'type' => 'ignore',
                                'nullable' => false,
                            ],
                            [
                                'name' => 'string',
                                'dbName' => 'string-db',
                                'type' => $stringType,
                                'nullable' => false,
                            ],
                            [
                                'name' => 'string-nullable',
                                'dbName' => 'string-nullable',
                                'type' => $stringType,
                                'nullable' => true,
                            ],
                            [
                                'name' => 'text-nullable',
                                'dbName' => 'text-nullable',
                                'type' => $stringType,
                                'nullable' => true,
                            ],
                        ],
                    ],
                ],
            ],
        ];

        file_put_contents($this->dataDir->getTmpFolder() . '/config.json', json_encode($config));

        copy(__DIR__ . '/../data/csv/types.csv', $this->dataDir->getTmpFolder() . '/in/tables/types.csv');

        $process = $this->runProcess();

        Assert::assertEquals(0, $process->getExitCode());
        Assert::assertMatchesRegularExpression('/File types.csv - Export finished/ui', $process->getOutput());

        // One batch
        Assert::assertEquals(1, $this->parseBatchCountFromOutput($process->getOutput()));

        // Wait for update
        sleep(2);

        // Check mapping
        $indices = $this->writer->getClient()->indices()->getMapping(['index' => $this->index]);
        // Response differs between versions,
        // https://www.elastic.co/guide/en/elasticsearch/reference/current/removal-of-types.html
        $mapping =
            $indices[$this->index]['mappings']['types']['properties'] ??
            $indices[$this->index]['mappings']['properties'];
        $mapping = array_map(function (array $item) {
            unset($item['fields']);
            return $item;
        }, $mapping);
        Assert::assertSame(
            [
                'bool-db' => ['type' => 'boolean'],
                'bool-nullable' => ['type' => 'boolean'],
                'double-db' => ['type' => 'double'],
                'float-nullable' => ['type' => 'float'],
                'int-nullable' => ['type' => 'integer'],
                'long-db' => ['type' => 'long'],
                'missing-in-config' => ['type' => $stringType],
                'string-db' => ['type' => $stringType],
                'string-nullable' => ['type' => $stringType],
                'text-nullable' => ['type' => $stringType],
            ],
            $mapping,
        );

        // Check values
        $response = $this->writer->getClient()->search(['_source' => true]);
        $hits = $response['hits']['hits'];
        usort($hits, function (array $a, array $b) {
            return $a['_id'] - $b['_id'];
        });
        Assert::assertSame([
            [
                '_index' => 'keboola_es_writer_test',
                '_type' => 'types',
                '_id' => '1',
                '_score' => 1.0,
                '_source' =>
                    [
                        'long-db' => 1,
                        'int-nullable' => 2,
                        'double-db' => 1.0,
                        'float-nullable' => 20.0,
                        'bool-db' => true,
                        'bool-nullable' => false,
                        'string-db' => 'def',
                        'string-nullable' => 'xyz',
                        'text-nullable' => 'xyz',
                        'missing-in-config' => 'xyz',
                    ],
            ],
            [
                '_index' => 'keboola_es_writer_test',
                '_type' => 'types',
                '_id' => '2',
                '_score' => 1.0,
                '_source' =>
                    [
                        'long-db' => 2,
                        'int-nullable' => null,
                        'double-db' => 0.0,
                        'float-nullable' => null,
                        'bool-db' => false,
                        'bool-nullable' => null,
                        'string-db' => '',
                        'string-nullable' => null,
                        'text-nullable' => null,
                        'missing-in-config' => '',
                    ],
            ],
            [
                '_index' => 'keboola_es_writer_test',
                '_type' => 'types',
                '_id' => '3',
                '_score' => 1.0,
                '_source' =>
                    [
                        'long-db' => 3,
                        'int-nullable' => 4,
                        'double-db' => 100.0,
                        'float-nullable' => 200.0,
                        'bool-db' => true,
                        'bool-nullable' => false,
                        'string-db' => 'def2',
                        'string-nullable' => 'xyz3',
                        'text-nullable' => 'xyz3',
                        'missing-in-config' => 'xyz3',
                    ],
            ],
        ], $hits);
    }
}
