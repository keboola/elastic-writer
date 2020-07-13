<?php
/**
 * @package wr-elasticsearch
 * @author Erik Zigo <erik.zigo@keboola.com>
 */
namespace Keboola\ElasticsearchWriter;

use Elasticsearch;
use Keboola\Csv\CsvFile;
use Keboola\ElasticsearchWriter\Exception\UserException;
use Keboola\ElasticsearchWriter\Options\LoadOptions;
use Monolog\Handler\TestHandler;
use Monolog\Logger;

class LoadTest extends AbstractTest
{
	public function testLogBulkErrors()
	{
		$writer = $this->writer;
		$testHandler = new TestHandler();

		$writer->enableLogger((new Logger($this->index))->setHandlers([$testHandler]));

		$options = new LoadOptions();
		$options->setIndex(strtoupper($this->index))
			->setType('language')
			->setBulkSize(LoadOptions::DEFAULT_BULK_SIZE);

		$csv1 = new CsvFile(__DIR__ .'/../../data/csv/' . $options->getType() .'.csv');

		try {
			$writer->loadFile($csv1, $options);
			$this->fail('Exception expected.');
		} catch (UserException $e) {
			$this->assertSame('Export failed.', $e->getMessage());
		}

		$errorsCount = 0;
		foreach ($testHandler->getRecords() as $record) {
			if ($record['level'] === 400){
				$errorsCount++;
			}
		}

		$this->assertEquals($this->countTable($csv1), $errorsCount);
	}

	/**
	 * Test bulk load
	 */
	public function testWriterWithDocumentId()
	{
		$writer = $this->writer;

		$options = new LoadOptions();
		$options->setIndex($this->index)
			->setType('language')
			->setBulkSize(LoadOptions::DEFAULT_BULK_SIZE);

		$csv1 = new CsvFile(__DIR__ .'/../../data/csv/' . $options->getType() .'.csv');
		$writer->loadFile($csv1, $options, 'id');

		$csv2 = new CsvFile(__DIR__ .'/../../data/csv/' . $options->getType() .'-update.csv');
		$writer->loadFile($csv2, $options, 'id');

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

	/**
	 * Test bulk load
	 */
	public function testWriterWithInvalidDocumentId()
	{
		$writer = $this->writer;

		$options = new LoadOptions();
		$options->setIndex($this->index)
			->setType('language')
			->setBulkSize(LoadOptions::DEFAULT_BULK_SIZE);

		$csv1 = new CsvFile(__DIR__ .'/../../data/csv/' . $options->getType() .'.csv');

		$this->expectException(UserException::class);
		$this->expectExceptionMessage('CSV error: Missing id column "fakeId" on line "2".');
		$writer->loadFile($csv1, $options, 'fakeId');
	}

	/**
	 * Test bulk load
	 */
	public function testWriterWithDocumentIdTwice()
	{
		$writer = $this->writer;

		$options = new LoadOptions();
		$options->setIndex($this->index)
			->setType('language')
			->setBulkSize(LoadOptions::DEFAULT_BULK_SIZE);

		$csv1 = new CsvFile(__DIR__ .'/../../data/csv/' . $options->getType() .'.csv');
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

	/**
	 * Test bulk load
	 */
	public function testWriterTwice()
	{
		$writer = $this->writer;

		$options = new LoadOptions();
		$options->setIndex($this->index)
			->setType('language')
			->setBulkSize(LoadOptions::DEFAULT_BULK_SIZE);

		$csv1 = new CsvFile(__DIR__ .'/../../data/csv/' . $options->getType() .'.csv');
		$writer->loadFile($csv1, $options, null);
		$writer->loadFile($csv1, $options, null);

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
		$this->assertEquals($this->countTable($csv1) * 2, $count['count']);
	}
}
