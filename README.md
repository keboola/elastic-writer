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
        
# Kebooola Connection

Elasticsearch Writer is integrated in Keboola Connection.

![image](https://cloud.githubusercontent.com/assets/1726727/13111357/a66b4674-d585-11e5-92cd-f8ff11fe1ebf.png)

Available with standard KB Docker Generic UI

![image](https://cloud.githubusercontent.com/assets/1726727/13111467/3cde9994-d586-11e5-83ca-00caefb22a2e.png)
# test
