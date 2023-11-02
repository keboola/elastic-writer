<?php

declare(strict_types=1);

namespace Keboola\ElasticsearchWriter\Tests;

use Keboola\Component\Logger;
use Keboola\Csv\CsvReader;
use Keboola\ElasticsearchWriter\Writer;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use Symfony\Component\Process\Process;

abstract class AbstractTestClass extends TestCase
{
    protected ?LoggerInterface $logger = null;

    protected Writer $writer;

    protected string $index = 'keboola_es_writer_test';

    protected Temp $dataDir;

    protected function setUp(): void
    {
        $this->writer = new Writer(
            sprintf('%s:%s', getenv('EX_ES_HOST'), getenv('EX_ES_HOST_PORT')),
            $this->logger ?? new Logger(),
        );
        $this->dataDir = new Temp();
        mkdir($this->dataDir->getTmpFolder() . '/in/tables', 0777, true);
        mkdir($this->dataDir->getTmpFolder() . '/in/files', 0777, true);
        $this->cleanUp();
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        $this->cleanUp();

        # Close SSH tunnel if created
        $process = new Process(['sh', '-c', 'pgrep ssh | xargs -r kill']);
        $process->mustRun();
    }

    protected function cleanUp(): void
    {
        // Remove all indexes
        foreach ($this->writer->getClient()->cat()->indices() as $data) {
            $response = $this->writer->getClient()->indices()->delete([
                'index' => $data['index'],
            ]);

            $this->assertArrayHasKey('acknowledged', $response);
            $this->assertTrue($response['acknowledged']);
        }

        $configPath = './tests/data/run/config.yml';
        if (file_exists($configPath)) {
            unlink($configPath);
        }
    }

    /**
     * Count records in CSV (with headers)
     */
    protected function countTable(CsvReader $file): int
    {
        $linesCount = 0;
        foreach ($file as $i => $line) {
            // skip header
            if (!$i) {
                continue;
            }

            $linesCount++;
        }

        return $linesCount;
    }

    protected function parseBatchCountFromOutput(string $output): int
    {
        preg_match_all('/Write .+ batch [0-9]+ to .+ start/ui', $output, $matches);
        return count($matches[0]);
    }

    protected function runProcess(): Process
    {
        $process = Process::fromShellCommandline('php /code/src/run.php');
        $process->setEnv([
            'KBC_DATADIR' => $this->dataDir->getTmpFolder(),
        ]);
        $process->run();

        return $process;
    }
}
