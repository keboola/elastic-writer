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
	 * @param CsvFile $file
	 * @param LoadOptions $options
	 * @param $primaryIndex
	 * @return bool
	 */
	public function loadFile(CsvFile $file, LoadOptions $options, $primaryIndex)
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

			$params['body'][] = [
				'index' => [
					'_index' => $options->getIndex(),
					'_type' => $options->getType(),
					'_id' => $lineData[$primaryIndex]
				]
			];

			$params['body'][] = $lineData;

			if ($i % $options->getBulkSize() == 0) {
				$this->logger->info(sprintf(
					"Write %s batch %d to %s",
					$options->getType(),
					$iBulk,
					$options->getIndex()
				));
				$responses = $this->client->bulk($params);

				$params = ['body' => []];

				if ($responses['errors'] !== false) {
					if (!empty($responses['items'])) {
						foreach ($responses['items'] as $itemResult) {
							if (!empty($itemResult['index']['error'])) {
								$this->logger->error(sprintf("ES error: %s", $itemResult['index']['error']));
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
				"Write %s batch %d to %s",
				$options->getType(),
				$iBulk,
				$options->getIndex()
			));
			$responses = $this->client->bulk($params);

			if ($responses['errors'] !== false) {
				if (!empty($responses['items'])) {
					foreach ($responses['items'] as $itemResult) {
						if (!empty($itemResult['index']['error'])) {
							$this->logger->error(sprintf("ES error: %s", $itemResult['index']['error']));
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