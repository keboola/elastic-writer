<?php

declare(strict_types=1);

namespace Keboola\ElasticsearchWriter;

use Keboola\ElasticsearchWriter\Configuration\Config;
use Keboola\ElasticsearchWriter\Exception\UserException;
use Keboola\SSHTunnel\SSH;
use Keboola\SSHTunnel\SSHException;
use Psr\Log\LoggerInterface;

class WriterFactory
{
    public function __construct(readonly Config $config, readonly LoggerInterface $logger)
    {
    }

    public function createWriter(): Writer
    {
        $parameters = $this->config->getParameters();

        if (isset($parameters['elastic']['ssh']['enabled']) && $parameters['elastic']['ssh']['enabled']) {
            $parameters = $this->createSshTunnel($parameters);
        }

        $host = sprintf(
            '%s:%s',
            $parameters['elastic']['host'],
            $parameters['elastic']['port'],
        );

        return new Writer(
            $host,
            $this->logger,
            $parameters['elastic']['username'] ?? null,
            $parameters['elastic']['#password'] ?? null,
        );
    }

    private function createSshTunnel(array $parameters): array
    {
        $sshConfig = $parameters['elastic']['ssh'];

        if (empty($sshConfig['user'])) {
            $sshConfig['user'] = $parameters['user'];
        }

        if (empty($sshConfig['remoteHost'])) {
            $sshConfig['remoteHost'] = $parameters['elastic']['host'];
        }

        if (empty($sshConfig['remotePort'])) {
            $sshConfig['remotePort'] = $parameters['elastic']['port'];
        }

        if (empty($sshConfig['localPort'])) {
            $sshConfig['localPort'] = 19200;
        }

        if (empty($sshConfig['sshPort'])) {
            $sshConfig['sshPort'] = 22;
        }

        $sshConfig['privateKey'] = $sshConfig['keys']['private'];

        $tunnelParams = array_intersect_key($sshConfig, array_flip([
            'user', 'sshHost', 'sshPort', 'localPort', 'remoteHost', 'remotePort', 'privateKey',
        ]));

        $this->logger->info("Creating SSH tunnel to '" . $tunnelParams['sshHost'] . "'");

        $ssh = new SSH();

        try {
            $ssh->openTunnel($tunnelParams);
        } catch (SSHException $e) {
            throw new UserException($e->getMessage());
        }

        $parameters['elastic']['host'] = '127.0.0.1';
        $parameters['elastic']['port'] = $sshConfig['localPort'];

        return $parameters;
    }
}
