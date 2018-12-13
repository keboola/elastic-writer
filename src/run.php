<?php
/**
 * @package wr-elasticsearch
 * @author Erik Zigo <erik.zigo@keboola.com>
 */
use Symfony\Component\Yaml\Yaml;
use Keboola\ElasticsearchWriter\Exception;
use Elasticsearch\Common\Exceptions\ElasticsearchException;
use Keboola\ElasticsearchWriter\Validator;
use Keboola\ElasticsearchWriter\Application;
use Monolog\Logger;
use Monolog\Handler\StreamHandler;
use Monolog\Handler\NullHandler;
use Monolog\Formatter\LineFormatter;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

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

	//@FIXME move to application, refactor with symfony config mapping
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

	// move encrypted params to non-ecrypted
	if (isset($config['elastic']['#host'])) {
		$config['elastic']['host'] = $config['elastic']['#host'];
		unset($config['elastic']['#host']);
	}

	if (isset($config['elastic']['ssh']['keys']['#private'])) {
		$config['elastic']['ssh']['keys']['private'] = $config['elastic']['ssh']['keys']['#private'];
		unset($config['elastic']['ssh']['keys']['#private']);
	}

	// data path
	$config['path'] = $arguments["data"] . '/in/tables';

	$app = new Application($config, $logger);
	$result = $app->run($action, $config);

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
} catch (\Throwable $e) {
	if ($e instanceof ElasticsearchException) {
		$logger->error('ES error: ' . $e->getMessage(), array());

		if ($action !== 'run') {
			echo $e->getMessage();
		}

		exit(1);
	} else {
		$logger->critical(
			get_class($e) . ':' . $e->getMessage(),
			[
				'errFile' => $e->getFile(),
				'errLine' => $e->getLine(),
				'errCode' => $e->getCode(),
				'errTrace' => $e->getTraceAsString(),
				'errPrevious' => $e->getPrevious() ? get_class($e->getPrevious()) : '',
			]
		);
		exit(2);
	}
}
