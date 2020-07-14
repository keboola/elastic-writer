<?php
/**
 * @package wr-elasticsearch
 * @author Erik Zigo <erik.zigo@keboola.com>
 */
namespace Keboola\ElasticsearchWriter\Validator;

use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class ConfigValidator
{
	/**
	 * @param array $config
	 * @return bool
	 * @throws InvalidConfigurationException
	 */
	private static function elasticConfigValidation(array $config)
	{
		// required params
		if (!isset($config['host']) && !isset($config['#host'])) {
			throw new InvalidConfigurationException('Missing elastic host');
		}
		if (!isset($config['port'])) {
			throw new InvalidConfigurationException('Missing elastic port');
		}

		return true;
	}

	private static function sshConfigValidation(array $config)
	{
		// required params
		if (!isset($config['keys'])) {
			throw new InvalidConfigurationException('Missing SSH keys');
		}
		if (!isset($config['sshHost'])) {
			throw new InvalidConfigurationException('Missing SSH host');
		}
		if (!isset($config['user'])) {
			throw new InvalidConfigurationException('Missing SSH user');
		}
		if (!isset($config['keys']['private']) && !isset($config['keys']['#private'])) {
			throw new InvalidConfigurationException('Missing SSH private key');
		}

		return true;
	}

	/**
	 * @param array $config
	 * @return bool
	 * @throws InvalidConfigurationException
	 */
	private static function tablesConfigValidation(array $config)
	{
		foreach ($config AS $i => $tableConfig) {
			$logPrefix = sprintf('Table config %d: ', $i + 1);

			if (!is_array($tableConfig)) {
				throw new InvalidConfigurationException($logPrefix . 'Config must be array');

			}

			if (!isset($tableConfig['tableId']) && !isset($tableConfig['file'])) {
				throw new InvalidConfigurationException($logPrefix . 'Missing file or tableId');
			}
			if (!isset($tableConfig['index'])) {
				throw new InvalidConfigurationException($logPrefix . 'Missing elastic index');
			}
			if (!isset($tableConfig['type'])) {
				throw new InvalidConfigurationException($logPrefix . 'Missing elastic document type');
			}
		}

		return true;
	}

	/**
	 * @param array $config
	 * @return bool
	 * @throws InvalidConfigurationException
	 */
	private static function columnsConfigValidation(array $tables)
	{
		foreach ($tables as $tableIndex => $tableConfig) {
			foreach ($tableConfig['items'] ?? [] as $columnIndex => $columnConfig) {
				$logPrefix = sprintf('Table config %d, column %d: ', $tableIndex + 1, $columnIndex + 1);

				if (empty($columnConfig['name'])) {
					throw new InvalidConfigurationException($logPrefix . 'Missing "name" key.');
				}

				if (empty($columnConfig['dbName'])) {
					throw new InvalidConfigurationException($logPrefix . 'Missing "dbName" key.');
				}

				if (empty($columnConfig['type'])) {
					throw new InvalidConfigurationException($logPrefix . 'Missing "type" key.');
				}

				if (!isset($columnConfig['nullable'])) {
					throw new InvalidConfigurationException($logPrefix . 'Missing "nullable" key.');
				}

				if (!is_bool($columnConfig['nullable'])) {
					throw new InvalidConfigurationException($logPrefix . '"nullable" key must be boolean.');
				}
			}
		}


		return true;
	}

	/**
	 * @param array $config
	 * @return bool
	 * @throws InvalidConfigurationException
	 */
	public static function validate(array $config)
	{
		if (!isset($config['parameters']) || !is_array($config['parameters'])) {
			throw new InvalidConfigurationException('Missing parameters configuration');
		}

		$config = $config['parameters'];

		// elastic validation
		if (!isset($config['elastic'])) {
			throw new InvalidConfigurationException('Missing elasticsearch configuration');
		}

		self::elasticConfigValidation($config['elastic']);

		// elastic validation
		if (!isset($config['tables']) || !is_array($config['tables'])) {
			throw new InvalidConfigurationException('Missing tables config');
		}

		self::tablesConfigValidation($config['tables']);

		if (isset($config['elastic']['ssh'])) {
			self::sshConfigValidation($config['elastic']['ssh']);
		}

		self::columnsConfigValidation($config['tables']);

		return true;
	}
}
