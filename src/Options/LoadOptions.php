<?php

declare(strict_types=1);

namespace Keboola\ElasticsearchWriter\Options;

use Keboola\ElasticsearchWriter\Mapping\ColumnsMapper;

class LoadOptions
{
    public const DEFAULT_BULK_SIZE = 10000;

    private string $index;

    private string $type;

    private int $bulkSize = self::DEFAULT_BULK_SIZE;

    private array $columns = [];

    public function setIndex(string $value): self
    {
        $this->index = $value;
        return $this;
    }

    public function getIndex(): string
    {
        return $this->index;
    }

    public function setType(string $value): self
    {
        $this->type = $value;
        return $this;
    }

    public function getType(): string
    {
        return $this->type;
    }

    public function setBulkSize(int $value): self
    {
        $this->bulkSize = $value;
        return $this;
    }

    public function getBulkSize(): int
    {
        return $this->bulkSize;
    }

    public function setColumns(array $columns): self
    {
        $this->columns = $columns;
        return $this;
    }


    public function getColumns(): array
    {
        return $this->columns;
    }

    public function getColumnsMapper(): ColumnsMapper
    {
        return new ColumnsMapper($this->getColumns());
    }
}
