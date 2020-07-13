<?php
/**
 * @package wr-elasticsearch
 * @author Erik Zigo <erik.zigo@keboola.com>
 */
namespace Keboola\ElasticsearchWriter;

use Elasticsearch;

class ConnectionTest extends AbstractTest
{
	/**
	 * @var Elasticsearch\Client
	 */
	protected $client;

	protected function setUp()
	{
		parent::setUp();
		$builder = Elasticsearch\ClientBuilder::create();
		$builder->setHosts(array(sprintf('%s:%s', getenv('EX_ES_HOST'), getenv('EX_ES_HOST_PORT'))));
		$this->client = $builder->build();
	}

	/**
	 * Test ES connection
	 */
	public function testConnection()
	{
		$this->assertTrue($this->client->ping());
	}
}
