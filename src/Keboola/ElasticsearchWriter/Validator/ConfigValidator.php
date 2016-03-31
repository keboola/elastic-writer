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

		return true;
	}
}