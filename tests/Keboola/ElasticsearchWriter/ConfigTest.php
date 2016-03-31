<?php
/**
 * @package wr-elasticsearch
 * @author Erik Zigo <erik.zigo@keboola.com>
 */
namespace Keboola\ElasticsearchWriter;

use Elasticsearch;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;
use Symfony\Component\Yaml\Yaml;

class ConfigTest extends AbstractTest
{
	/**
	 * Test valid configs
	 */
	public function testValidConfigs()
	{
		$path = __DIR__ .'/../../data/config/valid';

		$files = scandir($path);
		foreach ($files AS $file) {
			if (!preg_match('/\.yml$/ui', $file)) {
				continue;
			}

			$config = Yaml::parse(file_get_contents($path . "/" . $file));

			$result = Validator\ConfigValidator::validate($config);
			$this->assertTrue($result);
		}
	}

	/**
	 * Test invalid configs
	 */
	public function testInvalidConfigs()
	{
		$path = __DIR__ .'/../../data/config/invalid';

		$files = scandir($path);
		foreach ($files AS $file) {
			if (!preg_match('/\.yml$/ui', $file)) {
				continue;
			}

			$config = Yaml::parse(file_get_contents($path . "/" . $file));

			try {
				$result = Validator\ConfigValidator::validate($config);
				$this->assertFalse($result, sprintf('Config file %s should be invalid', $file));

				$this->fail(sprintf('Config file %s should be invalid', $file));
			} catch (InvalidConfigurationException $e) {
			}
		}
	}
}