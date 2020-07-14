<?php
/**
 * @package wr-elasticsearch
 * @author Erik Zigo <erik.zigo@keboola.com>
 */
namespace Keboola\ElasticsearchWriter;

use Elasticsearch;
use Keboola\Csv\CsvFile;
use Keboola\ElasticsearchWriter\Exception\UserException;
use Keboola\ElasticsearchWriter\Mapping\ColumnsMapper;
use Keboola\SSHTunnel\SSH;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class Application
{
	/**
	 * @var Writer
	 */
	private $writer;

	/**
	 * @var LoggerInterface
	 */
	private $logger;

	public function __construct(array $parameters, LoggerInterface $logger = null)
	{
		$this->logger = ($logger) ?: new NullLogger();

		if (isset($parameters['elastic']['ssh']['enabled']) && $parameters['elastic']['ssh']['enabled']) {
			$parameters = $this->createSshTunnel($parameters);
		}

		$host = sprintf(
			'%s:%s', $parameters['elastic']['host'], $parameters['elastic']['port']
		);

		$this->writer = new Writer($host);
		$this->writer->enableLogger($this->logger);
	}

	private function createSshTunnel($parameters)
	{
		$sshConfig = $parameters['elastic']['ssh'];

//		// check params
//		foreach (['keys', 'sshHost', 'user'] as $k) {
//			if (empty($sshConfig[$k])) {
//				throw new UserException(sprintf("Parameter %s is missing.", $k));
//			}
//		}

		if (empty($sshConfig['user'])) {
			$sshConfig['user'] = $parameters['user'];
		}

		if (empty($sshConfig['remoteHost'])) {
			$sshConfig['remoteHost'] = $parameters['elastic']['host'];
		}

		if (empty($sshConfig['remotePort'])) {
			$sshConfig['remotePort'] = $parameters['elastic']['port'];
		}

		if (empty($sshConfig['localPort'])) {
			$sshConfig['localPort'] = 19200;
		}

		if (empty($sshConfig['sshPort'])) {
			$sshConfig['sshPort'] = 22;
		}

		$sshConfig['privateKey'] = $sshConfig['keys']['private'];


		$tunnelParams = array_intersect_key($sshConfig, array_flip([
			'user', 'sshHost', 'sshPort', 'localPort', 'remoteHost', 'remotePort', 'privateKey'
		]));

		$this->logger->info("Creating SSH tunnel to '" . $tunnelParams['sshHost'] . "'");

		$ssh = new SSH();

		$ssh->openTunnel($tunnelParams);

		$parameters['elastic']['host'] = '127.0.0.1';
		$parameters['elastic']['port'] = $sshConfig['localPort'];

		return $parameters;
	}

	public function run($action, array $parameters)
	{
		$actionMethod = $action . 'Action';
		if (!method_exists($this, $actionMethod)) {
			throw new UserException(sprintf("Action '%s' does not exist.", $this['action']));
		}

		try {
			$this->writer->getClient()->ping();
		} catch (\Exception $e) {
			throw new Exception\UserException("Connection to elasticsearch failed");
		}

		return $this->$actionMethod($parameters);
	}


	public function runAction($parameters)
	{
		$path = $parameters["path"];

		$skipped = 0;
		$processed = 0;

		foreach ($parameters['tables'] AS $table) {
			$sourceType = !empty($table['tableId']) ? 'table' : 'file';

			if ($sourceType == 'table') {
				$logPrefix = sprintf('Table %s - ', $table['tableId']);
			} else {
				$logPrefix = sprintf('File %s - ', $table['file']);
			}

			if (empty($table['export'])) {
				$this->logger->info($logPrefix . 'Skipped');
				$skipped++;
				continue;
			}

			$this->logger->info($logPrefix . 'Export start');

			// load options
			$options = new Options\LoadOptions();
			$options
				->setIndex($table['index'])
				->setType($table['type'])
				->setColumns($table['items'] ?? []);

			if (!empty($parameters['elastic']['bulkSize'])) {
				$options->setBulkSize($parameters['elastic']['bulkSize']);
			}

			$idColumn = !empty($table['id']) ? $table['id'] : null;


			// source file
			if (!empty($table['tableId'])) {
				$file = new CsvFile(sprintf('%s/%s.csv', $path, $table['tableId']));
			} else {
				$file = new CsvFile(sprintf('%s/%s', $path, $table['file']));

				if (mb_strtolower($file->getExtension()) !== 'csv') {
					throw new Exception\UserException($logPrefix . 'Export failed. Only csv files are supported');
				}
			}

			if (!$file->isFile()) {
				throw new Exception\UserException($logPrefix . 'Export failed. Missing csv file');
			}


			try {
				$this->writer->loadFile($file, $options, $idColumn);
			} catch (UserException $e) {
				// Add prefix to message
				throw new UserException($logPrefix . $e->getMessage());
			}
			$this->logger->info($logPrefix . 'Export finished', array());

			$processed++;
		}

		$this->logger->info(sprintf("Exported %d tables. %d was skipped", $processed, $skipped));
	}

	public function mappingAction()
	{
		$return = ['indices' => []];

		foreach ($this->writer->listIndices() AS $indice) {
			$return['indices'][] = [
				'id' => $indice['id'],
				'mappings' => $this->writer->listIndiceMappings($indice['id']),
			];
		}

		return $return;
	}
}
