# Testando ingestão de dados com Prefect para GCS

## Instalação

```
pip install -e .
```

```
make all
```

## Prefect

### Initialize

```
prefect project init
```
this generates prefect.yaml and .prefectignore

In prefect.yaml, go the the deployments section and change deployment parameters.

### Setup your work pool
```
prefect worker start -p <WORK-POOL-NAME> -t process
```

### Login
```
prefect cloud login
```

### Run your flow

```
python write_to_gcs
```

### Deploy 

```
prefect deploy --all
```


### Set GitHub Actions: Prefect secrets

View your credentials 

```
prefect config view --show-secrets
```

Create PREFECT_API_KEY and PREFECT_API_URL as Secrets in your repository
