# de-etl-house_pricing

* Requires Python 3.6+


## To test locally, Run:

```
make start
```

# Make a request to the container
```
curl -X POST http://localhost:9000/2015-03-31/functions/function/invocations -H 'Content-Type: application/json' -d '"{}"'
```

## To run all tests, execute:
```
pytest tests/
```