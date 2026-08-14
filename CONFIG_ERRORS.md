# yamp Config Error Index

When a config document fails validation, `yamp-config validate` and `yamp-doctor`
report one of these field-failure causes with a stable slug, a fix hint, and a
link here. This file is generated from the single-source catalog in the `config`
module (both arms); regenerate it with `python/tools/gen_error_index.py`.

| Slug | Description | Fix |
| --- | --- | --- |
| [not-object](#not-object) | the config is not a JSON object | wrap the settings in a top-level { ... } object |
| [backends-not-object](#backends-not-object) | 'backends' is not a JSON object | make 'backends' a map of id to { address } |
| [invalid-backend-id](#invalid-backend-id) | a backend id is empty or contains the reserved '__' delimiter | rename the backend so its id has no '__' |
| [backend-no-addresses](#backend-no-addresses) | a backend declares no address | give the backend an 'address' or a non-empty 'addresses' |
| [missing-listen](#missing-listen) | the config has no 'listen' address | add "listen": "127.0.0.1:PORT" |
| [unknown-collision-strategy](#unknown-collision-strategy) | namespacing.strategy is not a supported strategy | set it to prefix, priority, manual, or passthrough |
| [invalid-handler-id](#invalid-handler-id) | a rest handler id is missing or invalid | give the handler a non-empty id without '__' |
| [handler-backend-collision](#handler-backend-collision) | a handler id collides with a backend id | rename the handler or the backend |
| [handler-missing-baseurl](#handler-missing-baseurl) | a rest handler has no 'baseUrl' | add a 'baseUrl' to the handler |
| [invalid-json](#invalid-json) | the config is not valid JSON | fix the JSON syntax at the reported line and column |

### not-object

the config is not a JSON object.

Fix. wrap the settings in a top-level { ... } object

### backends-not-object

'backends' is not a JSON object.

Fix. make 'backends' a map of id to { address }

### invalid-backend-id

a backend id is empty or contains the reserved '__' delimiter.

Fix. rename the backend so its id has no '__'

### backend-no-addresses

a backend declares no address.

Fix. give the backend an 'address' or a non-empty 'addresses'

### missing-listen

the config has no 'listen' address.

Fix. add "listen": "127.0.0.1:PORT"

### unknown-collision-strategy

namespacing.strategy is not a supported strategy.

Fix. set it to prefix, priority, manual, or passthrough

### invalid-handler-id

a rest handler id is missing or invalid.

Fix. give the handler a non-empty id without '__'

### handler-backend-collision

a handler id collides with a backend id.

Fix. rename the handler or the backend

### handler-missing-baseurl

a rest handler has no 'baseUrl'.

Fix. add a 'baseUrl' to the handler

### invalid-json

the config is not valid JSON.

Fix. fix the JSON syntax at the reported line and column
