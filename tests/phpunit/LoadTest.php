<?php

declare(strict_types=1);

namespace Keboola\ElasticsearchWriter\Tests;

use Keboola\Csv\CsvReader;
use Keboola\ElasticsearchWriter\Exception\UserException;
use Keboola\ElasticsearchWriter\Options\LoadOptions;
use Monolog\Handler\TestHandler;
use Monolog\Logger;

class LoadTest extends AbstractTestClass
{
    private TestHandler $testHandler;

    protected function setUp(): void
    {
        $this->testHandler = new TestHandler();
        $logger = new Logger($this->index);
        $logger->pushHandler($this->testHandler);

        $this->logger = $logger;
        parent::setUp();
    }

    public function testLogBulkErrors(): void
	{
		$options = new LoadOptions();
		$options->setIndex(strtoupper($this->index))
			->setType('language')
			->setBulkSize(LoadOptions::DEFAULT_BULK_SIZE);

		$csv1 = new CsvReader(__DIR__ .'/../data/csv/' . $options->getType() .'.csv');

		try {
            $this->writer->loadFile($csv1, $options);
			$this->fail('Exception expected.');
		} catch (UserException $e) {
			$this->assertSame('Export failed.', $e->getMessage());
		}

		$errorsCount = 0;
		foreach ($this->testHandler->getRecords() as $record) {
			if ($record['level'] === 400){
				$errorsCount++;
			}
		}

		$this->assertEquals($this->countTable($csv1), $errorsCount);
	}

	public function testWriterWithDocumentId(): void
	{
		$writer = $this->writer;

		$options = new LoadOptions();
		$options->setIndex($this->index)
			->setType('language')
			->setBulkSize(LoadOptions::DEFAULT_BULK_SIZE);

		$csv1 = new CsvReader(__DIR__ .'/../data/csv/' . $options->getType() .'.csv');
        $this->writer->loadFile($csv1, $options, 'id');

		$csv2 = new CsvReader(__DIR__ .'/../data/csv/' . $options->getType() .'-update.csv');
        $this->writer->loadFile($csv2, $options, 'id');

		// test if index exists
		$params = ['index' => $options->getIndex()];
		$settings = $writer->getClient()->indices()->getSettings($params);

		$this->assertCount(1, $settings);
		$this->assertArrayHasKey($options->getIndex(), $settings);
		$this->assertArrayHasKey('settings', $settings[$options->getIndex()]);
		$this->assertArrayHasKey('index', $settings[$options->getIndex()]['settings']);

		$writer->getClient()->indices()->refresh(['index' => $options->getIndex()]);

		$params = [
			'index' => $options->getIndex(),
			'type' => $options->getType(),
		];

		$count = $writer->getClient()->count($params);

		$this->assertArrayHasKey('count', $count);
		$this->assertEquals($this->countTable($csv1) + $this->countTable($csv2) - 1, $count['count']);
	}

	public function testWriterWithInvalidDocumentId(): void
	{
		$writer = $this->writer;

		$options = new LoadOptions();
		$options->setIndex($this->index)
			->setType('language')
			->setBulkSize(LoadOptions::DEFAULT_BULK_SIZE);

		$csv1 = new CsvReader(__DIR__ .'/../data/csv/' . $options->getType() .'.csv');

		$this->expectException(UserException::class);
		$this->expectExceptionMessage('CSV error: Missing id column "fakeId" on line "2".');
		$writer->loadFile($csv1, $options, 'fakeId');
	}

	public function testWriterWithDocumentIdTwice(): void
	{
		$writer = $this->writer;

		$options = new LoadOptions();
		$options->setIndex($this->index)
			->setType('language')
			->setBulkSize(LoadOptions::DEFAULT_BULK_SIZE);

		$csv1 = new CsvReader(__DIR__ .'/../data/csv/' . $options->getType() .'.csv');
		$writer->loadFile($csv1, $options, 'id');
		$writer->loadFile($csv1, $options, 'id');

		// test if index exists
		$params = ['index' => $options->getIndex()];
		$settings = $writer->getClient()->indices()->getSettings($params);

		$this->assertCount(1, $settings);
		$this->assertArrayHasKey($options->getIndex(), $settings);
		$this->assertArrayHasKey('settings', $settings[$options->getIndex()]);
		$this->assertArrayHasKey('index', $settings[$options->getIndex()]['settings']);

		$writer->getClient()->indices()->refresh(['index' => $options->getIndex()]);

		$params = [
			'index' => $options->getIndex(),
			'type' => $options->getType(),
		];

		$count = $writer->getClient()->count($params);

		$this->assertArrayHasKey('count', $count);
		$this->assertEquals($this->countTable($csv1), $count['count']);
	}

	public function testWriterTwice(): void
	{
		$options = new LoadOptions();
		$options->setIndex($this->index)
			->setType('language')
			->setBulkSize(LoadOptions::DEFAULT_BULK_SIZE);

		$csv1 = new CsvReader(__DIR__ .'/../data/csv/' . $options->getType() .'.csv');
        $this->writer->loadFile($csv1, $options);
        $this->writer->loadFile($csv1, $options);

		// test if index exists
		$params = ['index' => $options->getIndex()];
		$settings = $this->writer->getClient()->indices()->getSettings($params);

		$this->assertCount(1, $settings);
		$this->assertArrayHasKey($options->getIndex(), $settings);
		$this->assertArrayHasKey('settings', $settings[$options->getIndex()]);
		$this->assertArrayHasKey('index', $settings[$options->getIndex()]['settings']);

        $this->writer->getClient()->indices()->refresh(['index' => $options->getIndex()]);

		$params = [
			'index' => $options->getIndex(),
			'type' => $options->getType(),
		];

		$count = $this->writer->getClient()->count($params);

		$this->assertArrayHasKey('count', $count);
		$this->assertEquals($this->countTable($csv1), $count['count']);
	}
}
