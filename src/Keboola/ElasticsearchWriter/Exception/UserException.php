<?php
/**
 * @package wr-elasticsearch
 * @author Erik Zigo <erik.zigo@keboola.com>
 */
namespace Keboola\ElasticsearchWriter\Exception;

class UserException extends \InvalidArgumentException
{
	const ERR_DATA_PARAM = 'Data folder not set.';
	const ERR_MISSING_CONFIG = 'Missing configuration file.';
}