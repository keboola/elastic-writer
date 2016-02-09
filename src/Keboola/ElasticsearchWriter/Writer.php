<?php
/**
 * @package wr-elasticsearch
 * @author Erik Zigo <erik.zigo@keboola.com>
 */
namespace Keboola\ElasticsearchWriter;

use Elasticsearch;
use Keboola\Csv\CsvFile;
use Keboola\ElasticsearchWriter\Options\LoadOptions;

class Writer
{
	/**
	 * @var Elasticsearch\Client
	 */
	private $client;

	public function __construct($host)
	{
		$builder = Elasticsearch\ClientBuilder::create();
		$builder->setHosts(array($host));
		$this->client = $builder->build();
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
				$responses = $this->client->bulk($params);

				$params = ['body' => []];

				if ($responses['errors'] !== false) {
					return false;
				}

				unset($responses);
			}
		}

		if (!empty($params['body'])) {
			$responses = $this->client->bulk($params);

			if ($responses['errors'] !== false) {
				return false;
			}

			unset($responses);
		}

		return true;
	}
}