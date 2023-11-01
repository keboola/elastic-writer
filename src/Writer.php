<?php

declare(strict_types=1);

namespace Keboola\ElasticsearchWriter;

use Elasticsearch\Client;
use Elasticsearch\ClientBuilder;
use Generator;
use Iterator;
use Keboola\Csv\CsvReader;
use Keboola\ElasticsearchWriter\Exception\UserException;
use Keboola\ElasticsearchWriter\Mapping\ColumnsMapper;
use Keboola\ElasticsearchWriter\Options\LoadOptions;
use LimitIterator;
use NoRewindIterator;
use Psr\Log\LoggerInterface;
use Throwable;

class Writer
{
    private Client $client;

    public function __construct(
        string $host,
        private readonly LoggerInterface $logger,
        ?string $user = null,
        ?string $password = null,
    ) {
        $builder = ClientBuilder::create();
        $builder->setHosts([$host]);
        if ($user && $password) {
            $builder->setBasicAuthentication($user, $password);
        }
        $this->client = $builder->build();
    }

    public function getClient(): Client
    {
        return $this->client;
    }

    public function loadFile(CsvReader $file, LoadOptions $options, ?string $primaryIndex = null): void
    {
        $csvHeader = $file->getHeader();

        try {
            $this->createIndexIfNotExists($options);
        } catch (Throwable $e) {
            $this->logger->warning(sprintf('Index create error: %s. Ignored.', $e->getMessage()));
        }

        $iterator = new NoRewindIterator($file);
        $iterator->next(); // skip header
        $bulkIndex = 1;
        while ($iterator->valid()) {
            $bulk = new LimitIterator($iterator, 0, $options->getBulkSize());
            $body = iterator_to_array($this->mapRowsToRequestBody($options, $csvHeader, $bulk, $primaryIndex));
            $this->sendBulkRequest($body, $bulkIndex, $options);
            $bulkIndex++;
        }
    }

    private function mapRowsToRequestBody(
        LoadOptions $options,
        array $csvHeader,
        Iterator $rows,
        ?string $primaryIndex = null,
    ): Generator {
        foreach ($rows as $line => $values) {
            $row = iterator_to_array($options->getColumnsMapper()->mapCsvRow($csvHeader, (array) $values));

            $indexBody = [
                '_index' => $options->getIndex(),
                '_type' => $options->getType(),
            ];

            if ($primaryIndex) {
                if (!array_key_exists($primaryIndex, $row)) {
                    throw new UserException(
                        sprintf('CSV error: Missing id column "%s" on line "%s".', $primaryIndex, $line + 1),
                    );
                }

                $indexBody['_id'] = $row[$primaryIndex];
            }

            yield ['index' => $indexBody];
            yield $row;
        }
    }

    private function sendBulkRequest(array $body, int $bulkIndex, LoadOptions $options): void
    {
        $this->logger->info(sprintf(
            'Write %s batch %d to %s start',
            $options->getType(),
            $bulkIndex,
            $options->getIndex(),
        ));
        $responses = $this->client->bulk(['body' => $body]);

        $this->logger->info(sprintf(
            'Write %s batch %d to %s took %d ms',
            $options->getType(),
            $bulkIndex,
            $options->getIndex(),
            $responses['took'],
        ));

        if ($responses['errors'] !== false) {
            if (!empty($responses['items'])) {
                foreach ($responses['items'] as $itemResult) {
                    $operation = key($itemResult);

                    if ($itemResult[$operation]['status'] >= 400) {
                        $this->logItemError($itemResult[$operation]);
                    }
                }
            }

            throw new UserException('Export failed.');
        }
    }

    private function createIndexIfNotExists(LoadOptions $options): void
    {
        // Create index only if not exists
        $indexExists = $this->client->indices()->exists(['index' => $options->getIndex()]);
        if ($indexExists) {
            return;
        }

        // Prepare properties
        $properties = [];
        $columns = $options->getColumnsMapper()->getAllColumns();
        foreach ($columns as $column) {
            // Ignore ignored columns
            if ($column->getType() === ColumnsMapper::IGNORED_COLUMN_TYPE) {
                continue;
            }

            $properties[$column->getDbName()] = ['type' => $column->getType()];
        }
        // Send request
        if ($properties) {
            $this->client->indices()->create([
                'index' => $options->getIndex(),
                'include_type_name' => true,
                'body' => ['mappings' => [$options->getType() => ['properties' => $properties]]],
            ]);
        }
    }

    /**
     * Creates error message string from error field
     * @param array<string, string> $error
     */
    private function getErrorMessageFromErrorField(array $error): string
    {
        $message = [];
        foreach (['type', 'reason'] as $key) {
            if (isset($error[$key])) {
                $message[] = $error[$key];
            }
        }
        return implode('; ', $message);
    }

    /**
     * @return array{id: int|string}[]
     */
    public function listIndices(): array
    {
        $return = [];

        $stats = $this->client->indices()->stats();
        if (!empty($stats['indices'])) {
            foreach (array_keys($stats['indices']) as $indice) {
                $return[] = ['id' => $indice];
            }
        }

        return $return;
    }

    /**
     * @return array{id: int|string}[]
     */
    public function listIndiceMappings(string $indice): array
    {
        $return = [];

        $stats = $this->client->indices()->getMapping(['index' => $indice]);

        if (!empty($stats[$indice]) && !empty($stats[$indice]['mappings'])) {
            foreach (array_keys($stats[$indice]['mappings']) as $mapping) {
                $return[] = ['id' => $mapping];
            }
        }

        return $return;
    }

    /**
     * @param array{error: string|array, _id: string} $item
     */
    private function logItemError(array $item): void
    {
        if (!empty($item['error'])) {
            if (is_array($item['error'])) {
                $this->logger->error(sprintf(
                    "ES error(document ID '%s'): %s",
                    $item['_id'],
                    $this->getErrorMessageFromErrorField($item['error']),
                ));
            } else {
                $this->logger->error(sprintf(
                    "ES error(document ID '%s'): %s",
                    $item['_id'],
                    $item['error'],
                ));
            }
        }
    }
}
