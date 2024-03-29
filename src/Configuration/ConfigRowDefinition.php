<?php

declare(strict_types=1);

namespace Keboola\ElasticsearchWriter\Configuration;

use Keboola\Component\Config\BaseConfigDefinition;
use Keboola\ElasticsearchWriter\Configuration\Node\ElasticNode;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class ConfigRowDefinition extends BaseConfigDefinition
{
    protected function getParametersDefinition(): ArrayNodeDefinition
    {
        $parametersNode = parent::getParametersDefinition();
        // @formatter:off
        /** @noinspection NullPointerExceptionInspection */
        $parametersNode
            ->ignoreExtraKeys(false)
            ->children()
                ->append(new ElasticNode())
                ->scalarNode('id')->end()
                ->scalarNode('index')->isRequired()->cannotBeEmpty()->end()
                ->scalarNode('type')->isRequired()->cannotBeEmpty()->end()
                ->booleanNode('export')->defaultTrue()->end()
                ->arrayNode('items')->arrayPrototype()
                    ->children()
                        ->scalarNode('name')->isRequired()->cannotBeEmpty()->end()
                        ->scalarNode('type')->isRequired()->cannotBeEmpty()->end()
                        ->scalarNode('dbName')->isRequired()->cannotBeEmpty()->end()
                        ->booleanNode('nullable')->defaultTrue()->end()
                    ->end()
                ->end()
            ->end()
        ;
        // @formatter:on
        return $parametersNode;
    }
}
