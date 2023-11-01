<?php

declare(strict_types=1);

namespace Keboola\ElasticsearchWriter\Tests;

use Elasticsearch\Client;
use Elasticsearch\ClientBuilder;

class ConnectionTest extends AbstractTestClass
{
	protected Client $client;

	protected function setUp(): void
    {
		parent::setUp();
		$builder = ClientBuilder::create();
		$builder->setHosts(array(sprintf('%s:%s', getenv('EX_ES_HOST'), getenv('EX_ES_HOST_PORT'))));
		$this->client = $builder->build();
	}

	public function testConnection(): void
	{
		$this->assertTrue($this->client->ping());
	}
}
