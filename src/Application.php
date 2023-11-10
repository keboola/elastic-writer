<?php

declare(strict_types=1);

namespace Keboola\ElasticsearchWriter;

use Keboola\CommonExceptions\UserExceptionInterface;
use Keboola\Component\BaseComponent;
use Keboola\Component\JsonHelper;
use Keboola\Csv\CsvReader;
use Keboola\ElasticsearchWriter\Configuration\Config;
use Keboola\ElasticsearchWriter\Configuration\ConfigActionDefinition;
use Keboola\ElasticsearchWriter\Configuration\ConfigDefinition;
use Keboola\ElasticsearchWriter\Configuration\ConfigRowDefinition;
use Keboola\ElasticsearchWriter\Exception\UserException;
use Keboola\ElasticsearchWriter\Options\LoadOptions;

class Application extends BaseComponent
{
    private array $rawConfig;

    private bool $baseConfigDefinition = true;

    /**
     * @throws UserExceptionInterface
     */
    protected function run(): void
    {
        $parameters = $this->getConfig()->getParameters();
        $writerFactory = new WriterFactory($this->getConfig(), $this->getLogger());
        $writer = $writerFactory->createWriter();

        $this->baseConfigDefinition = false;

        if (!empty($parameters['tables'])) {
            $tables = $parameters['tables'];
            unset($parameters['tables']);
            foreach ($tables as $table) {
                $this->rawConfig = ['parameters' => $parameters + $table];
                $this->loadConfig();
                $this->runRow($writer);
            }
        } else {
            $this->loadConfig();
            $this->runRow($writer);
        }
    }

    private function runRow(Writer $writer): void
    {
        $parameters = $this->getConfig()->getParameters();

        $host = sprintf(
            '%s:%s',
            $parameters['elastic']['host'],
            $parameters['elastic']['port'],
        );

        $sourceType = !empty($parameters['tableId']) ? 'table' : 'file';
        $path = $this->getDataDir() . '/in/tables';

        if ($sourceType === 'table') {
            $logPrefix = sprintf('Table %s - ', $parameters['tableId']);
        } else {
            $logPrefix = sprintf('File %s - ', $parameters['file']);
        }

        if (empty($parameters['export'])) {
            $this->getLogger()->info($logPrefix . 'Skipped');
            return;
        }

        $this->getLogger()->info($logPrefix . 'Export start');

        // load options
        $options = new LoadOptions();
        $options
            ->setIndex($parameters['index'])
            ->setType($parameters['type'])
            ->setColumns($parameters['items'] ?? []);

        if (!empty($parameters['elastic']['bulkSize'])) {
            $options->setBulkSize($parameters['elastic']['bulkSize']);
        }

        $idColumn = !empty($parameters['id']) ? $parameters['id'] : null;

        // source file
        if (!empty($parameters['tableId'])) {
            $file = new CsvReader(sprintf('%s/%s.csv', $path, $parameters['tableId']));
        } else {
            $file = new CsvReader(sprintf('%s/%s', $path, $parameters['file']));
            if (!str_ends_with(mb_strtolower($parameters['file']), 'csv')) {
                throw new UserException($logPrefix . 'Export failed. Only csv files are supported');
            }
        }

        try {
            $writer->loadFile($file, $options, $idColumn);
        } catch (UserException $e) {
            // Add prefix to message
            throw new UserException($logPrefix . $e->getMessage());
        }
        $this->getLogger()->info($logPrefix . 'Export finished');
    }




    protected function getConfigDefinitionClass(): string
    {
        $rawConfig = $this->getRawConfig();
        $action = $rawConfig['action'] ?? 'run';

        if ($action !== 'run') {
            return ConfigActionDefinition::class;
        } elseif ($this->baseConfigDefinition) {
            return ConfigDefinition::class;
        } else {
            return ConfigRowDefinition::class;
        }
    }

    protected function getRawConfig(): array
    {
        if (!empty($this->rawConfig)) {
            return $this->rawConfig;
        }
        return JsonHelper::readFile($this->getDataDir() . '/config.json');
    }

    protected function getSyncActions(): array
    {
        return [
            'mapping' => 'mappingAction',
        ];
    }

    public function getConfig(): Config
    {
        /** @var Config $config */
        $config = parent::getConfig();
        return $config;
    }

    protected function getConfigClass(): string
    {
        return Config::class;
    }

    public function mappingAction(): array
    {
        $writerFactory = new WriterFactory($this->getConfig(), $this->getLogger());
        $writer = $writerFactory->createWriter();

        $return = ['indices' => []];

        foreach ($writer->listIndices() as $indice) {
            $return['indices'][] = [
                'id' => $indice['id'],
                'mappings' => $writer->listIndiceMappings((string) $indice['id']),
            ];
        }

        return $return;
    }
}
