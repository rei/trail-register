# Trail Register

Trail Register is a usage tracking tool. Named after the log books placed at trailheads that are used partly to track trail usage.

## Getting Started

Trail Register can be run via docker or with the standalone jar directly.

**Docker:**

    docker run -d -p 4567:4567 -v /data/trail-register:/data reicoop/trail-register

**Java**

    java -jar trail-register.jar

## Clustering

Trail register can also be run in a clustered mode for high availability. In this mode writes are independent to each node in the cluster,
since this a write heavy system. Reads are done by the node doing the same read to all of its peers and adding them together.

Load balancing must be handled externally by some other tool such as HA-Proxy, nginx, or an F5.

**Docker:**              

    docker run -d -p 4567:4567 -e PEERS=${HOST}:4567,${HOST}:4568 reicoop/trail-register
    docker run -d -p 4568:4567 -e PEERS=${HOST}:4567,${HOST}:4568 reicoop/trail-register

## REST Endpoints

##### `GET /`
lists applications<br>
**Example Response:** ["app1", "app2"]

##### `GET /$app`
lists environments for an application<br>
**Example Response:** `["env1", "env2"]`

##### `GET /$app/$env`
lists categories used in an environment<br>
**Example Response:** `["category1", "category2"]`

##### `GET /$app/$env/$category[?days=30]`
lists all usages by key for a given category.<br>
**Example Response:** `{"key1": 7, "key2":25}`

##### `GET /$app/$env/$category/$key[?days=30][&by_date=false]`
lists all usages for given key for a given category.<br>
**Example Response:** `12`<br>
**Example Response (by date):** `{"20150910": 7, "20150919":5}`

##### `POST /$app/$env/$category/$key`
records a single usage for the given key

##### `POST /$app/$env/$category`
records usages for the given keys<br>
**Example Request:** `{"key1": 7, "key2":5}`

##### `POST /$app/$env`
records usages for the given keys by category<br>
**Example Request:** `{"category1": {"key1": 7, "key2":5}, "category2": {"key1": 4, "key2":8}}`

##### `GET /health`
**Example Response:** `UP`

##### `GET /_stats`
returns timing info per endpoint
