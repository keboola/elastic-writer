<?php

declare(strict_types=1);

namespace Keboola\ElasticsearchWriter\Configuration;

use Keboola\Component\Config\BaseConfigDefinition;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class ConfigActionDefinition extends BaseConfigDefinition
{
    protected function getParametersDefinition(): ArrayNodeDefinition
    {
        $parametersNode = parent::getParametersDefinition();
        // @formatter:off
        /** @noinspection NullPointerExceptionInspection */
        $parametersNode
            ->ignoreExtraKeys()
            ->children()
                ->arrayNode('elastic')->isRequired()
                    ->validate()->always(function ($elastic) {
                        if (!isset($elastic['host']) && !isset($elastic['#host'])) {
                            throw new InvalidConfigurationException(
                                'Elastic config must contain either "#host" key.',
                            );
                        }
                        if (isset($elastic['host']) && isset($elastic['#host'])) {
                            throw new InvalidConfigurationException(
                                'Elastic config must contain either "host" or "#host" key, not both.',
                            );
                        }
                        if (isset($elastic['#host'])) {
                            $elastic['host'] = $elastic['#host'];
                            unset($elastic['#host']);
                        }
                        return $elastic;
                    })->end()
                    ->children()
                        ->scalarNode('host')->end()
                        ->scalarNode('#host')->end()
                        ->scalarNode('port')->isRequired()->end()
                        ->integerNode('bulkSize')->min(1)->end()
                        ->arrayNode('ssh')
                            ->children()
                                ->booleanNode('enabled')->end()
                                ->scalarNode('sshHost')->isRequired()->cannotBeEmpty()->end()
                                ->scalarNode('sshPort')->end()
                                ->scalarNode('localPort')->end()
                                ->scalarNode('user')->isRequired()->cannotBeEmpty()->end()
                                ->arrayNode('keys')->isRequired()
                                    ->validate()->always(function ($keys) {
                                        if (!isset($keys['#private']) && !isset($keys['private'])) {
                                            throw new InvalidConfigurationException(
                                                'SSH keys config must contain either "#private" key.',
                                            );
                                        }
                                        if (isset($keys['#private']) && isset($keys['private'])) {
                                            throw new InvalidConfigurationException(
                                                'SSH keys config must contain either ' .
                                                '"private" or "#private" key, not both.',
                                            );
                                        }
                                        if (isset($keys['#private'])) {
                                            $keys['private'] = $keys['#private'];
                                            unset($keys['#private']);
                                        }
                                        return $keys;
                                    })->end()
                                    ->children()
                                        ->scalarNode('private')->end()
                                        ->scalarNode('#private')->end()
                                    ->end()
                                ->end()
                            ->end()
                        ->end()
                    ->end()
                ->end()
            ->end()
        ;
        // @formatter:on
        return $parametersNode;
    }
}
