# `meltano-ssm-parameter-state-backend`

 ⚠️ **EXPERIMENTAL: See https://github.com/meltano/meltano/tree/state-backend-plugins.** ⚠️

## Configuration

### `meltano.yml`

```yaml
state_backend:
  uri: ssm-parameter://meltano/state
```

### Environment Variables

* `MELTANO_STATE_BACKEND_URI`: The URI of the base path for meltano state value in SSM.
