<?php

declare(strict_types=1);

namespace Keboola\ElasticsearchWriter\Mapping;

class ColumnMapping
{
    private string $csvName;

    private string $dbName;

    private string $type;

    private bool $nullable;

    public function __construct(array $data)
    {
        $this->csvName = $data['name'];
        $this->dbName = $data['dbName'];
        $this->type = $data['type'];
        $this->nullable = $data['nullable'];
    }

    public function getCsvName(): string
    {
        return $this->csvName;
    }

    public function getDbName(): string
    {
        return $this->dbName;
    }

    public function getType(): string
    {
        return $this->type;
    }

    public function isNullable(): bool
    {
        return $this->nullable;
    }
}
