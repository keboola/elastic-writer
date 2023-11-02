<?php

declare(strict_types=1);

namespace Keboola\ElasticsearchWriter\Tests;

use Keboola\ElasticsearchWriter\Configuration\Config;
use Keboola\ElasticsearchWriter\Configuration\ConfigDefinition;
use PHPUnit\Framework\Assert;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class ConfigTest extends AbstractTestClass
{
    public function testValidConfigs(): void
    {
        $path = __DIR__ . '/../data/config/valid';

        $files = (array) scandir($path);
        foreach ($files as $file) {
            if (!preg_match('/\.json$/ui', (string) $file)) {
                continue;
            }

            $configRaw = (array) json_decode((string) file_get_contents($path . '/' . $file), true);

            $config = new Config($configRaw, new ConfigDefinition());
            Assert::assertNotEmpty($config);
        }
    }

    public function testInvalidConfigs(): void
    {
        $path = __DIR__ . '/../data/config/invalid';

        $files = (array) scandir($path);
        foreach ($files as $file) {
            if (!preg_match('/\.json$/ui', (string) $file)) {
                continue;
            }

            $configRaw = (array) json_decode((string) file_get_contents($path . '/' . $file), true);

            try {
                new Config($configRaw, new ConfigDefinition());
                $this->fail(sprintf('Config file %s should be invalid', $file));
            } catch (InvalidConfigurationException $e) {
                Assert::assertTrue(true);
            }
        }
    }
}
