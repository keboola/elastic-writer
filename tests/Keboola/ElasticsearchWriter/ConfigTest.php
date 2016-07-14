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
	public function validConfigsData()
	{
		$return = array();
		$path = __DIR__ .'/../../data/config/valid';

		$files = scandir($path);
		foreach ($files AS $file) {
			if (preg_match('/\.yml$/ui', $file)) {
				$return[]  = [$path . "/" . $file];
			}
		}

		return $return;
	}

	public function invalidConfigsData()
	{
		$return = array();
		$path = __DIR__ .'/../../data/config/invalid';

		$files = scandir($path);
		foreach ($files AS $file) {
			if (preg_match('/\.yml$/ui', $file)) {
				$return[]  = [$path . "/" . $file];
			}
		}

		return $return;
	}

	/**
	 * Test valid configs
	 *
	 * @dataProvider validConfigsData
	 */
	public function testValidConfigs($filePath)
	{
		$config = Yaml::parse($filePath);

		$result = Validator\ConfigValidator::validate($config);
		$this->assertTrue($result);
	}

	/**
	 * Test invalid configs
	 *
	 * @dataProvider invalidConfigsData
	 */
	public function testInvalidConfigs($filePath)
	{
		$fileInfo = new \SplFileInfo($filePath);
		$config = Yaml::parse($filePath);

		try {
			$result = Validator\ConfigValidator::validate($config);
			$this->assertFalse($result, sprintf('Config file %s should be invalid', $fileInfo->getFilename()));

			$this->fail(sprintf('Config file %s should be invalid', $fileInfo->getFilename()));
		} catch (InvalidConfigurationException $e) {
		}
	}
}