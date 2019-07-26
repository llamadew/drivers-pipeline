# drivers-pipeline
This repo contains an ETL script that executes postprocessing for of the raw client metadata telemetry into an analytical dataset.
The resulting dataset resides in an MDB collection. 

## How does the ETL process data? 
The ETL aggregates client connections count daily by `group ID`, `driver`,`driver version`, `platform`, `OS`, `OSarchitecture`, `MDB server version`. 
It further parses the platform field for drivers that provide additional information in their handshake metadata. 

## Fields definitions
| Field name | Field Definition | Type | Example |
| ---------- | ---------------- | ---- | ------- |
| _id | Object ID | ObjectID | ObjectID("5d39e040aaa881a39eb2245e") |
| ts | Timestamp | ISODate | 2019-07-24T23:58:39.973+00:00
| c | Count of connections | Integer | 7 |
| d | Driver Name | String | "nodejs" |
| dv | Driver Version | String | "3.2.7" |
| gid | GroupID | ObjectID | ObjectID("5b07281d4e65811062c710d7") |
| os | Client OS Name | String | "linux" |
| osa | Client OS Architecture | String | "x64" |
| sv | MDB Version | String | "3.6.13" |
| d | Day of aggregation | Int | 1 |
| m | Month of aggregation | Int | 8 |
| y | Year of aggregation | Int | 2019 |
| lver | Driver Language Version | String or Object (nullable) | "10.12.0" |
| fr | Framework | String (nullable) | "mongoid-5.2.1" |

