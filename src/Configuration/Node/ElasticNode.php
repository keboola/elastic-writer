<?php

declare(strict_types=1);

namespace Keboola\ElasticsearchWriter\Configuration\Node;

use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Builder\ExprBuilder;
use Symfony\Component\Config\Definition\Builder\NodeBuilder;
use Symfony\Component\Config\Definition\Builder\NodeParentInterface;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class ElasticNode extends ArrayNodeDefinition
{
    public const NODE_NAME = 'elastic';

    public function __construct(?NodeParentInterface $parent = null)
    {
        parent::__construct(self::NODE_NAME, $parent);
        $this->isRequired();
        $this->validate();
        $this->init($this->children());
    }

    public function init(NodeBuilder $nodeBuilder): void
    {
        $this->addHostNode($nodeBuilder);
        $this->addPortNode($nodeBuilder);
        $this->addUsernameNode($nodeBuilder);
        $this->addPasswordNode($nodeBuilder);
        $this->addBulkSizeNode($nodeBuilder);
        $this->addSshNode($nodeBuilder);
    }

    public function validate(): ExprBuilder
    {
        return parent::validate()->always(function ($elastic) {
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
        });
    }

    protected function addHostNode(NodeBuilder $builder): void
    {
        $builder->scalarNode('host')->end();
        $builder->scalarNode('#host')->end();
    }

    protected function addPortNode(NodeBuilder $builder): void
    {
        $builder->scalarNode('port')->isRequired()->end();
    }

    protected function addUsernameNode(NodeBuilder $builder): void
    {
        $builder->scalarNode('username')->cannotBeEmpty()->end();
    }

    protected function addPasswordNode(NodeBuilder $builder): void
    {
        $builder->scalarNode('#password')->cannotBeEmpty()->end();
    }

    protected function addBulkSizeNode(NodeBuilder $builder): void
    {
        $builder->integerNode('bulkSize')->min(1)->end();
    }

    protected function addSshNode(NodeBuilder $builder): void
    {
        $builder->arrayNode('ssh')
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
                                'SSH keys config must contain either '.
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
        ->end();
    }
}
