<?php
/**
 * @package wr-elasticsearch
 * @author Erik Zigo <erik.zigo@keboola.com>
 */
namespace Keboola\ElasticsearchWriter;

use Elasticsearch;
use Keboola\Csv\CsvFile;
use Keboola\ElasticsearchWriter\Options\LoadOptions;

class LoadTest extends AbstractTest
{
	/**
	 * @var Writer
	 */
	protected $writer;

	protected $index = 'keboola_es_writer_test';

	protected function setUp()
	{
		$this->writer = new Writer(sprintf('%s:%s', getenv('EX_ES_HOST'), getenv('EX_ES_HOST_PORT')));

		$this->cleanUp();
	}

	/**
	 * Cleanup test workspace
	 *
	 * @throws Elasticsearch\Common\Exceptions\Missing404Exception
	 * @throws \Exception
	 */
	protected function cleanUp()
	{
		$params = ['index' => $this->index];

		if ($this->writer->getClient()->indices()->exists($params)) {
			$response = $this->writer->getClient()->indices()->delete($params);

			$this->assertArrayHasKey('acknowledged', $response);
			$this->assertTrue($response['acknowledged']);
		}
	}

	public function testBulkLoad()
	{
		$testBulkSize = 10;
		$writer = $this->writer;

		$options = new LoadOptions();
		$options->setIndex($this->index)
			->setType('language')
			->setBulkSize($testBulkSize);

		$csv1 = new CsvFile(__DIR__ .'/../../data/csv/' . $options->getType() .'-large.csv');
		$csv1ItemsCount = $this->countTable($csv1);

		$result = $writer->loadFile($csv1, $options, 'id');

		$this->assertInstanceOf('Keboola\ElasticsearchWriter\Result\LoadResult', $result);
		$this->assertEquals($csv1ItemsCount, $result->getDocCount());
		$this->assertEquals(ceil($csv1ItemsCount / $testBulkSize), $result->getBulkCount());

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
		$this->assertEquals($csv1ItemsCount, $count['count']);
	}

	/**
	 * Test bulk load
	 */
	public function testWriterWithDocumentId()
	{
		$testBulkSize = LoadOptions::DEFAULT_BULK_SIZE;
		$writer = $this->writer;

		$options = new LoadOptions();
		$options->setIndex($this->index)
			->setType('language')
			->setBulkSize($testBulkSize);

		$csv1 = new CsvFile(__DIR__ .'/../../data/csv/' . $options->getType() .'.csv');
		$csv1ItemsCount = $this->countTable($csv1);

		$result = $writer->loadFile($csv1, $options, 'id');

		$this->assertInstanceOf('Keboola\ElasticsearchWriter\Result\LoadResult', $result);
		$this->assertEquals($csv1ItemsCount, $result->getDocCount());
		$this->assertEquals(ceil($csv1ItemsCount / $testBulkSize), $result->getBulkCount());

		$csv2 = new CsvFile(__DIR__ .'/../../data/csv/' . $options->getType() .'-update.csv');
		$csv2ItemsCount = $this->countTable($csv2);

		$result = $writer->loadFile($csv2, $options, 'id');

		$this->assertInstanceOf('Keboola\ElasticsearchWriter\Result\LoadResult', $result);
		$this->assertEquals($csv2ItemsCount, $result->getDocCount());
		$this->assertEquals(ceil($csv2ItemsCount / $testBulkSize), $result->getBulkCount());

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
		$this->assertEquals($csv1ItemsCount + $csv2ItemsCount - 1, $count['count']);
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
		$result = $writer->loadFile($csv1, $options, 'fakeId');

		$this->assertFalse($result);
	}

	/**
	 * Test bulk load
	 */
	public function testWriterWithDocumentIdTwice()
	{
		$testBulkSize = LoadOptions::DEFAULT_BULK_SIZE;
		$writer = $this->writer;

		$options = new LoadOptions();
		$options->setIndex($this->index)
			->setType('language')
			->setBulkSize($testBulkSize);

		$csv1 = new CsvFile(__DIR__ .'/../../data/csv/' . $options->getType() .'.csv');
		$csv1ItemsCount = $this->countTable($csv1);

		$result = $writer->loadFile($csv1, $options, 'id');

		$this->assertInstanceOf('Keboola\ElasticsearchWriter\Result\LoadResult', $result);
		$this->assertEquals($csv1ItemsCount, $result->getDocCount());
		$this->assertEquals(ceil($csv1ItemsCount / $testBulkSize), $result->getBulkCount());

		$result = $writer->loadFile($csv1, $options, 'id');

		$this->assertInstanceOf('Keboola\ElasticsearchWriter\Result\LoadResult', $result);
		$this->assertEquals($csv1ItemsCount, $result->getDocCount());
		$this->assertEquals(ceil($csv1ItemsCount / $testBulkSize), $result->getBulkCount());

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
		$this->assertEquals($csv1ItemsCount, $count['count']);
	}

	/**
	 * Test bulk load
	 */
	public function testWriterTwice()
	{
		$testBulkSize = LoadOptions::DEFAULT_BULK_SIZE;
		$writer = $this->writer;

		$options = new LoadOptions();
		$options->setIndex($this->index)
			->setType('language')
			->setBulkSize($testBulkSize);

		$csv1 = new CsvFile(__DIR__ .'/../../data/csv/' . $options->getType() .'.csv');
		$csv1ItemsCount = $this->countTable($csv1);

		$result = $writer->loadFile($csv1, $options, null);

		$this->assertInstanceOf('Keboola\ElasticsearchWriter\Result\LoadResult', $result);
		$this->assertEquals($csv1ItemsCount, $result->getDocCount());
		$this->assertEquals(ceil($csv1ItemsCount / $testBulkSize), $result->getBulkCount());

		$result = $writer->loadFile($csv1, $options, null);

		$this->assertInstanceOf('Keboola\ElasticsearchWriter\Result\LoadResult', $result);
		$this->assertEquals($csv1ItemsCount, $result->getDocCount());
		$this->assertEquals(ceil($csv1ItemsCount / $testBulkSize), $result->getBulkCount());

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