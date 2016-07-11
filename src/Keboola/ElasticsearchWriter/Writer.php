<?php
/**
 * @package wr-elasticsearch
 * @author Erik Zigo <erik.zigo@keboola.com>
 */
namespace Keboola\ElasticsearchWriter;

use Elasticsearch;
use Keboola\Csv\CsvFile;
use Keboola\ElasticsearchWriter\Options\LoadOptions;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class Writer
{
	/**
	 * @var Elasticsearch\Client
	 */
	private $client;

	/**
	 * @var LoggerInterface
	 */
	private $logger;

	public function __construct($host)
	{
		$builder = Elasticsearch\ClientBuilder::create();
		$builder->setHosts(array($host));
		$this->client = $builder->build();

		$this->logger = new NullLogger();
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
	 * List of all indices
	 * @return array
	 */
	public function listIndices()
	{
		$return = array();

		$stats = $this->client->indices()->stats(array('metric' => 'indices'));
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

	/**
	 * @param CsvFile $file
	 * @param LoadOptions $options
	 * @param $primaryIndex
	 * @return bool
	 */
	public function loadFile(CsvFile $file, LoadOptions $options, $primaryIndex = null)
	{
		$csvHeader = $file->getHeader();

		$params = ['body' => []];

		$iBulk = 1;
		foreach ($file AS $i => $line) {
			// skip header
			if (!$i) {
				continue;
			}

			$lineData = array_combine($csvHeader, $line);

			if ($primaryIndex) {
				if (!array_key_exists($primaryIndex, $lineData)) {
					$this->logger->error(sprintf("CSV error: Missing id column %s on line %s", $primaryIndex, $i + 1));
					return false;
				}

				$params['body'][] = [
					'index' => [
						'_index' => $options->getIndex(),
						'_type' => $options->getType(),
						'_id' => $lineData[$primaryIndex]
					]
				];
			} else {
				$params['body'][] = [
					'index' => [
						'_index' => $options->getIndex(),
						'_type' => $options->getType(),
					]
				];
			}

			$params['body'][] = $lineData;

			if ($i % $options->getBulkSize() == 0) {
				$this->logger->info(sprintf(
					"Write %s batch %d to %s start",
					$options->getType(),
					$iBulk,
					$options->getIndex()
				));
				$responses = $this->client->bulk($params);

				$this->logger->info(sprintf(
					"Write %s batch %d to %s took %d ms",
					$options->getType(),
					$iBulk,
					$options->getIndex(),
					$responses['took']
				));

				$params = ['body' => []];

				if ($responses['errors'] !== false) {
					if (!empty($responses['items'])) {
						foreach ($responses['items'] as $itemResult) {
							if (!empty($itemResult['index']['error'])) {
								$this->logger->error(sprintf("ES error: %s", $itemResult['index']['error']));
								return false;
							}
						}
					}

					return false;
				}
				
				$iBulk++;
				unset($responses);
			}
		}

		if (!empty($params['body'])) {
			$this->logger->info(sprintf(
				"Write %s batch %d to %s start",
				$options->getType(),
				$iBulk,
				$options->getIndex()
			));
			$responses = $this->client->bulk($params);

			$this->logger->info(sprintf(
				"Write %s batch %d to %s took %d ms",
				$options->getType(),
				$iBulk,
				$options->getIndex(),
				$responses['took']
			));

			if ($responses['errors'] !== false) {
				if (!empty($responses['items'])) {
					foreach ($responses['items'] as $itemResult) {
						if (!empty($itemResult['index']['error'])) {
							$this->logger->error(sprintf("ES error: %s", $itemResult['index']['error']));
							return false;
						}
					}
				}

				return false;
			}

			unset($responses);
		}

		return true;
	}
}