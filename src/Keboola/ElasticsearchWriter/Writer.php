<?php
/**
 * @package wr-elasticsearch
 * @author Erik Zigo <erik.zigo@keboola.com>
 */
namespace Keboola\ElasticsearchWriter;

use Keboola\ElasticsearchWriter\Mapping\ColumnsMapper;
use NoRewindIterator;
use Iterator;
use LimitIterator;
use Generator;
use Elasticsearch;
use Keboola\Csv\CsvFile;
use Keboola\ElasticsearchWriter\Exception\UserException;
use Keboola\ElasticsearchWriter\Options\LoadOptions;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class Writer
{
	/** @var Elasticsearch\Client */
	private $client;

	/** @var LoggerInterface */
	private $logger;

	/** @var string  */
	private $serverVersion;

	/**
	 * https://www.elastic.co/guide/en/elasticsearch/reference/current/removal-of-types.html
	 * @var bool
	 */
	private $typesSupportedByServer;

	public function __construct($host)
	{
		$builder = Elasticsearch\ClientBuilder::create();
		$builder->setHosts(array($host));
		$this->client = $builder->build();
		$this->logger = new NullLogger();
		$this->serverVersion = $this->client->info()['version']['number'];
		$this->typesSupportedByServer = version_compare($this->serverVersion, '7.0', '<');
	}

	public function enableLogger(LoggerInterface $logger)
	{
		$this->logger = $logger;
	}

	/**
	 * @return Elasticsearch\Client
	 */
	public function getClient()
	{
		return $this->client;
	}

	/**
	 * @param CsvFile $file
	 * @param LoadOptions $options
	 * @param $primaryIndex
	 * @return void
	 */
	public function loadFile(CsvFile $file, LoadOptions $options, $primaryIndex = null)
	{
		$csvHeader = $file->getHeader();

		try {
			$this->createIndexIfNotExists($options);
		} catch (\Exception $e) {
			$this->logger->warning(sprintf('Index create error: %s. Ignored.', $e->getMessage()));
		}

		$iterator = new NoRewindIterator($file);
		$iterator->next(); // skip header
		$bulkIndex = 1;
		while ($iterator->valid()) {
			$bulk = new LimitIterator($iterator, 0, $options->getBulkSize());
			$body = iterator_to_array($this->mapRowsToRequestBody($options, $csvHeader, $primaryIndex, $bulk));
			$this->sendBulkRequest($body, $bulkIndex, $options);
			$bulkIndex++;
		}
	}

	private function mapRowsToRequestBody(LoadOptions $options, array $csvHeader, $primaryIndex, Iterator $rows): Generator
	{
		foreach ($rows as $line => $values) {
			$row = iterator_to_array($options->getColumnsMapper()->mapCsvRow($csvHeader, $values));

			$indexBody= [
				'_index' => $options->getIndex(),
				'_type' => $options->getType()
			];

			if ($primaryIndex) {
				if (!array_key_exists($primaryIndex, $row)) {
					throw new UserException(
						sprintf('CSV error: Missing id column "%s" on line "%s".', $primaryIndex, $line + 1)
					);
				}

				$indexBody['_id'] = $row[$primaryIndex];
			}

			yield ['index' => $indexBody];
			yield $row;
		}
	}

	private function sendBulkRequest(array $body, int $bulkIndex, LoadOptions $options)
	{
		$this->logger->info(sprintf(
			"Write %s batch %d to %s start",
			$options->getType(),
			$bulkIndex,
			$options->getIndex()
		));
		$responses = $this->client->bulk(['body' => $body]);

		$this->logger->info(sprintf(
			"Write %s batch %d to %s took %d ms",
			$options->getType(),
			$bulkIndex,
			$options->getIndex(),
			$responses['took']
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

	private function createIndexIfNotExists(LoadOptions $options)
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
				'body' => ['mappings' => [$options->getType() => ['properties' => $properties]]]
			]);
		}
	}

	/**
	 * Creates error message string from error field
	 * @param array $error
	 * @return string
	 */
	private function getErrorMessageFromErrorField(array $error)
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
	 * List of all indices
	 * @return array
	 */
	public function listIndices()
	{
		$return = array();

		$stats = $this->client->indices()->stats();
		if (!empty($stats['indices'])) {
			foreach (array_keys($stats['indices']) AS $indice) {
				$return[] = array('id' => $indice);
			}
		}

		return $return;
	}

	/**
	 * List of all mappings in specified index
	 * @return array
	 */
	public function listIndiceMappings($indice)
	{
		$return = array();

		$stats = $this->client->indices()->getMapping(array('index' => $indice));

		if (!empty($stats[$indice]) && !empty($stats[$indice]['mappings'])) {
			foreach (array_keys($stats[$indice]['mappings']) AS $mapping) {
				$return[] = array('id' => $mapping);
			}
		}

		return $return;
	}

	private function logItemError(array $item)
	{
		if (!empty($item['error'])) {
			if (is_array($item['error'])) {
				$this->logger->error(sprintf(
					"ES error(document ID '%s'): %s",
					$item['_id'],
					$this->getErrorMessageFromErrorField($item['error'])
				));
			} else {
				$this->logger->error(sprintf(
					"ES error(document ID '%s'): %s",
					$item['_id'],
					$item['error']
				));
			}
		}
	}
}
