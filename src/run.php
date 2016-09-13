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
use Monolog\Handler\NullHandler;
use Monolog\Formatter\LineFormatter;
use Keboola\Csv\CsvFile;
use \Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

require_once(__DIR__ . "/../bootstrap.php");

const APP_NAME = 'wr-elasticsearch';

$logger = new Logger(APP_NAME, array(
	(new StreamHandler('php://stdout', Logger::INFO))->setFormatter(new LineFormatter()),
	(new StreamHandler('php://stderr', Logger::ERROR))->setFormatter(new LineFormatter()),
));

set_error_handler(
	function ($errno, $errstr, $errfile, $errline, array $errcontext) {
		if (0 === error_reporting()) {
			return false;
		}
		throw new ErrorException($errstr, 0, $errno, $errfile, $errline);
	}
);

$action = 'run';


try {
	$arguments = getopt("d::", ["data::"]);
	if (!isset($arguments["data"])) {
		throw new Exception\UserException(Exception\UserException::ERR_DATA_PARAM);
	}

	if (!file_exists($arguments["data"] . "/config.yml")) {
		throw new Exception\UserException(Exception\UserException::ERR_MISSING_CONFIG);
	}

	$config = Yaml::parse(file_get_contents($arguments["data"] . "/config.yml"));
	if (empty($config)) {
		print "Could not parse config file";
		exit(2);
	}

	try {
		Validator\ConfigValidator::validate($config);
	} catch (InvalidConfigurationException $e) {
		throw new Exception\UserException($e->getMessage());
	}

	$action = isset($config['action']) ? $config['action'] : $action;
	$config = $config['parameters'];

	if ($action !== 'run') {
		$logger->setHandlers(array(new NullHandler(Logger::INFO)));
	}

	$host = sprintf(
		'%s:%s',
		!empty($config['elastic']['#host']) ? $config['elastic']['#host'] : $config['elastic']['host'],
		$config['elastic']['port']
	);

	$config['path'] = $arguments["data"] . '/in/tables';

	$writer = new Writer($host);
	$writer->enableLogger($logger);

	try {
		$writer->getClient()->ping();
	} catch (\Exception $e) {
		throw new Exception\UserException("Connection to elasticsearch failed");
	}

	$result = $writer->run($action, $config);

	if ($action !== 'run') {
		echo json_encode($result);
	}

	$logger->info("Elasticsearch writer finished successfully.");
	exit(0);
} catch (Exception\UserException $e) {
	$logger->error($e->getMessage(), array());

	if ($action !== 'run') {
		echo $e->getMessage();
	}

	exit(1);
} catch (Exception\ExportException $e) {
	$logger->error($e->getMessage(), array());
	exit(2);
} catch (\Exception $e) {
	$logger->error($e->getMessage(), [
		'trace' => $e->getTraceAsString()
	]);
	exit(2);
}

