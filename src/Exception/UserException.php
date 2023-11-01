<?php

declare(strict_types=1);

namespace Keboola\ElasticsearchWriter\Exception;

use InvalidArgumentException;
use Keboola\CommonExceptions\UserExceptionInterface;

class UserException extends InvalidArgumentException implements UserExceptionInterface
{
    public const ERR_DATA_PARAM = 'Data folder not set.';
    public const ERR_MISSING_CONFIG = 'Missing configuration file.';
}
