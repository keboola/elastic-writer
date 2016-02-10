# Elasticsearch Writer
---

# Configuration

- Configuration has 2 parts - `elastic` and `tables`
- The `elastic` section defines connection info and import config
    - `host` - server address
    - `port` - elasticsearch listening port
    - `bulkSize` *(optional)* - size of a batch to upload to Elasticsearch *(default is 10.000)*
    
- The `tables` section defines database tables, their columns and their data types
    - `tableId` - StorageAPI table ID of the table we want to write into Elasticsearch (see https://github.com/keboola/docker-bundle/blob/master/ENVIRONMENT.md#input-mapping) for more info about Input Mapping
    - `index` - index name in ES
    - `type` - type of the data, determines the type in ES,
    - `id` - determines in which column of table is the document's ID/primary key
    - `export` - whether this table shall be exported to ES

## Example

        {
                "elastic": {
                    "host": "my.hostname.com",
                    "port": 9200,
                    "bulkSize": 10000
                },
                "tables": [
                    {
                        "tableId": "in.c-main.products",
                        "index": "production",
                        "type": "products",
                        "id": "id",
                        "export": true
                    }
                ]
        }
