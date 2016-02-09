<?php
/**
 * @package wr-elasticsearch
 * @author Erik Zigo <erik.zigo@keboola.com>
 */
use Symfony\Component\Yaml\Yaml;
use Keboola\ElasticsearchWriter;
use Keboola\Csv\CsvFile;

require_once(__DIR__ . "/../bootstrap.php");

const APP_NAME = 'wr-elasticsearch';

set_error_handler(
	function ($errno, $errstr, $errfile, $errline, array $errcontext) {
		if (0 === error_reporting()) {
			return false;
		}
		throw new ErrorException($errstr, 0, $errno, $errfile, $errline);
	}
);

$arguments = getopt("d::", array("data::"));

if (!isset($arguments["data"])) {
	print "Data folder not set.";
	exit(1);
}

$config = Yaml::parse(file_get_contents($arguments["data"] . "/config.yml"));


$config = $config['parameters'];
if (!isset($config['elastic']['host'])) {
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
	$config['elastic']['host'],
	$config['elastic']['port']
);

$path = $arguments["data"] . '/in/tables';

try {
	$writer = new ElasticsearchWriter\Writer($host);

	try {
		$writer->getClient()->ping();
	} catch (\Exception $e) {
		throw new ElasticsearchWriter\Exception\UserException("Connection to elasticsearch failed");
	}

	$skipped = 0;
	$processed = 0;

	foreach ($config['tables'] AS $table) {
		if (empty($table['export'])) {
			$skipped++;
			continue;
		}

		$file = new CsvFile(sprintf('%s/%s.csv', $path, $table['tableId']));
		if (!$file->isFile()) {
			$skipped++;
			continue;
		}

		$options = new ElasticsearchWriter\Options\LoadOptions();
		$options->setIndex($table['index'])
			->setType($table['type']);

		if (!empty($table['bulkSize'])) {
			$options->setBulkSize($table['bulkSize']);
		}

		$result = $writer->loadFile($file, $options, $table['id']);
		if (!$result) {
			throw new ElasticsearchWriter\Exception\ExportException("Export table " . $table['tableId'] . " failed");
		}

		$processed++;
	}

	print sprintf("Writer finished. Exported %d tables. %d was skipped", $processed, $skipped);
	exit(0);
} catch (ElasticsearchWriter\Exception\UserException $e) {
	print $e->getMessage();
	exit(1);
} catch (ElasticsearchWriter\Exception\ExportException $e) {
	print $e->getMessage();
	exit(2);
} catch (\Exception $e) {
	exit(2);
}

