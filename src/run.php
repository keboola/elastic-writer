<?php
/**
 * @package wr-elasticsearch
 * @author Erik Zigo <erik.zigo@keboola.com>
 */
use Symfony\Component\Yaml\Yaml;
use Keboola\ElasticsearchWriter\Exception;
use Keboola\ElasticsearchWriter\Writer;
use Keboola\ElasticsearchWriter\Options;
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

	if (!isset($config['parameters'])) {
		print "Missing parameters configuration";
		exit(1);
	}

	$config = $config['parameters'];
	if (!isset($config['elastic']['host']) && !isset($config['elastic']['#host'])) {
		print "Missing elastic host";
		exit(1);
	}
	if (!isset($config['elastic']['port'])) {
		print "Missing elastic port";
		exit(1);
	}
	if (!isset($config['tables'])) {
		print "Missing tables config";
		exit(1);
	}

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

	foreach ($config['tables'] AS $table) {
		if (empty($table['export'])) {
			$logger->info(sprintf("Table %s - skipped", $table['tableId']));
			$skipped++;
			continue;
		} else {
			$logger->info(sprintf("Table %s - export start", $table['tableId']));
		}

		$file = new CsvFile(sprintf('%s/%s.csv', $path, $table['tableId']));
		if (!$file->isFile()) {
			throw new Exception\UserException(sprintf("Table %s export failed. Missing csv file", $table['tableId']));
		}

		$options = new Options\LoadOptions();
		$options->setIndex($table['index'])
			->setType($table['type']);

		if (!empty($config['elastic']['bulkSize'])) {
			$options->setBulkSize($config['elastic']['bulkSize']);
		}

		$result = $writer->loadFile($file, $options, $table['id']);
		if (!$result) {
			throw new Exception\ExportException("Export table " . $table['tableId'] . " failed");
		} else {
			$logger->info(sprintf("Table %s - export finished", $table['tableId']), array());
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

