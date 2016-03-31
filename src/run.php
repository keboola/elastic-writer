<?php
/**
 * @package wr-elasticsearch
 * @author Erik Zigo <erik.zigo@keboola.com>
 */
use Symfony\Component\Yaml\Yaml;
use Keboola\ElasticsearchWriter\Exception;
use Keboola\ElasticsearchWriter\Writer;
use Keboola\ElasticsearchWriter\Options;
use Keboola\ElasticsearchWriter\Validator;
use Monolog\Logger;
use Monolog\Handler\StreamHandler;
use Monolog\Formatter\LineFormatter;
use Keboola\Csv\CsvFile;

require_once(__DIR__ . "/../bootstrap.php");

const APP_NAME = 'wr-elasticsearch';

$logger = new Logger(APP_NAME, array(
	(new StreamHandler('php://stdout', Logger::INFO))->setFormatter(new LineFormatter("%message%\n")),
	(new StreamHandler('php://stderr', Logger::ERROR))->setFormatter(new LineFormatter("%message%\n")),
));

set_error_handler(
	function ($errno, $errstr, $errfile, $errline, array $errcontext) {
		if (0 === error_reporting()) {
			return false;
		}
		throw new ErrorException($errstr, 0, $errno, $errfile, $errline);
	}
);

try {
	$arguments = getopt("d::", array("data::"));

	if (!isset($arguments["data"])) {
		print "Data folder not set.";
		exit(1);
	}

	$config = Yaml::parse(file_get_contents($arguments["data"] . "/config.yml"));
	if (empty($config)) {
		print "Could not parse config file";
		exit(2);
	}

	Validator\ConfigValidator::validate($config);

	$config = $config['parameters'];

	$host = sprintf(
		'%s:%s',
		!empty($config['elastic']['#host']) ? $config['elastic']['#host'] : $config['elastic']['host'],
		$config['elastic']['port']
	);

	$path = $arguments["data"] . '/in/tables';

	$writer = new Writer($host);
	$writer->enableLogger($logger);

	try {
		$writer->getClient()->ping();
	} catch (\Exception $e) {
		throw new Exception\UserException("Connection to elasticsearch failed");
	}

	$skipped = 0;
	$processed = 0;

	$requiredParams = array();

	foreach ($config['tables'] AS $table) {
		$sourceType = !empty($table['tableId']) ? 'table' : 'file';

		if ($sourceType == 'table') {
			$logPrefix = sprintf('Table %s - ', $table['tableId']);
		} else {
			$logPrefix = sprintf('File %s - ', $table['file']);
		}

		if (empty($table['export'])) {
			$logger->info($logPrefix . 'Skipped');
			$skipped++;
			continue;
		}

		$logger->info($logPrefix . 'Export start');

		// load options
		$options = new Options\LoadOptions();
		$options->setIndex($table['index'])
			->setType($table['type']);

		if (!empty($config['elastic']['bulkSize'])) {
			$options->setBulkSize($config['elastic']['bulkSize']);
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

		$result = $writer->loadFile($file, $options, $idColumn);
		if (!$result) {
			throw new Exception\UserException($logPrefix . 'Export failed');
		} else {
			$logger->info($logPrefix . 'Export finished', array());
		}

		$processed++;
	}

	$logger->info(sprintf("Elasticsearch writer finished. Exported %d tables. %d was skipped", $processed, $skipped));
	exit(0);
} catch (Exception\UserException $e) {
	$logger->error($e->getMessage(), array());
	exit(1);
} catch (Exception\ExportException $e) {
	$logger->error($e->getMessage(), array());
	exit(2);
} catch (\Exception $e) {
	$logger->error($e->getMessage(), array());
	exit(2);
}

