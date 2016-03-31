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

		$csv1 = new CsvFile(__DIR__ .'/../../data/' . $options->getType() .'.csv');
		$result = $writer->loadFile($csv1, $options, 'id');

		$this->assertTrue($result);

		$csv2 = new CsvFile(__DIR__ .'/../../data/' . $options->getType() .'-update.csv');
		$result = $writer->loadFile($csv2, $options, 'id');

		$this->assertTrue($result);

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
	public function testWriterWithDocumentIdTwice()
	{
		$writer = $this->writer;

		$options = new LoadOptions();
		$options->setIndex($this->index)
			->setType('language')
			->setBulkSize(LoadOptions::DEFAULT_BULK_SIZE);

		$csv1 = new CsvFile(__DIR__ .'/../../data/' . $options->getType() .'.csv');
		$result = $writer->loadFile($csv1, $options, 'id');

		$this->assertTrue($result);

		$result = $writer->loadFile($csv1, $options, 'id');

		$this->assertTrue($result);

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

		$csv1 = new CsvFile(__DIR__ .'/../../data/' . $options->getType() .'.csv');
		$result = $writer->loadFile($csv1, $options, null);

		$this->assertTrue($result);

		$result = $writer->loadFile($csv1, $options, null);

		$this->assertTrue($result);

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