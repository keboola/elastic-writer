# Elasticsearch Writer

[![Build Status](https://travis-ci.org/keboola/elastic-writer.svg?branch=master)](https://travis-ci.org/keboola/elastic-writer)

Writer expects that mapping of types and indexes in your Elasticsearch exists. If it is missing and you have enabled [automatic index creation](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-index_.html#index-creation), new mapping will be created.

--

## Configuration

- Configuration has 2 parts - `elastic` and `tables`
- The `elastic` section defines connection info and import config
    - `host` - server address
    - `port` - elasticsearch listening port
    - `bulkSize` *(optional)* - size of a batch to upload to Elasticsearch *(default is 10.000)*
    - `ssh` - SSH tunnel configuration
        - `enabled` - enable SSH tunnel for connection to Elasticsearch
        -  `sshHost` - address of the SSH server
        -  `port` *(optional)* - SSH listening port *(default is 22)*
        -  `user` - SSH login
        -  `keys`
            - `private` - Your private key used for authentication  
- The `tables` section defines database tables, their columns and their data types
    - `file` or `tableId`
        - `file` - CSV file of the table we want to write into Elasticsearch (see https://github.com/keboola/docker-bundle/blob/master/ENVIRONMENT.md#input-mapping) for more info about Input Mapping
        - `tableId` - *(deprecated)* - ~~StorageAPI table ID of the table we want to write into Elasticsearch (see https://github.com/keboola/docker-bundle/blob/master/ENVIRONMENT.md#input-mapping) for more info about Input Mapping (Works only if *destination* attribute is not set in table configuration)~~
    - `index` - index name in ES
    - `type` - type of the data, determines the type in ES,
    - `id` *(optional)* - determines in which column of table is the document's ID/primary key
    - `export` - whether this table shall be exported to ES

### Example

```json
{
    "elastic": {
        "host": "my.hostname.com",
        "port": 9200,
        "bulkSize": 10000
    },
    "tables": [
        {
            "file": "products.csv",
            "index": "production",
            "type": "products",
            "id": "id",
            "export": true
        }
    ]
}
```

### Example with SSH

```json
{
    "elastic": {
        "host": "my.hostname.com",
        "port": 9200,
        "bulkSize": 10000,
        "ssh": {
            "sshHost": "10.112.1.1",
            "port": 22,
            "user": "extractor",
            "keys": {
                "private": "YOUR_PRIVATE_KEY_WITHOUT_PASSPHRASE"
            }
        }
    },
    "tables": [
        {
            "file": "products.csv",
            "index": "production",
            "type": "products",
            "id": "id",
            "export": true
        }
    ]
}
```

## Configuring in Keboola Connection

Elasticsearch Writer is integrated in Keboola Connection.

![image](https://cloud.githubusercontent.com/assets/1726727/13111357/a66b4674-d585-11e5-92cd-f8ff11fe1ebf.png)

Available with standard KB Docker Generic UI

![image](https://cloud.githubusercontent.com/assets/1726727/13111467/3cde9994-d586-11e5-83ca-00caefb22a2e.png)
