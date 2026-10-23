# Local Validation in the MWAA Image (Phase 6)

Parse DAGs and install requirements inside the
[amazon-mwaa-docker-images](https://github.com/aws/amazon-mwaa-docker-images)
image for the target Airflow version before any version jump. This is the only
pre-upgrade signal for Python standard-library removals and for user-code
deprecation warnings, which MWAA does not write to CloudWatch.

## Build the image

The repository builds images locally; there is no published registry image.
Resolve the version directory from the repository README at runtime.

```bash
git clone --depth 1 https://github.com/aws/amazon-mwaa-docker-images.git
cd amazon-mwaa-docker-images/images/airflow/<target-version>
docker build -f ./Dockerfiles/Dockerfile.base \
  -t amazon-mwaa-docker-images/airflow:<target-version>-base ./
```

- **Build for the host architecture.** On Apple silicon do not pass
  `--platform linux/amd64`: the bootstrap scripts select packages by the host
  build architecture, so an emulated amd64 build fails (for example on the
  MariaDB RPM step). DAG parsing does not depend on CPU architecture.
- **The base image is enough for parsing.** Some derivative Dockerfiles use a
  Podman-style `FROM localhost/...` reference that `docker buildx` tries to
  resolve from a registry. Use the `-base` image with an overridden entrypoint
  instead of building the derivatives.
- **Non-interactive Docker Desktop pulls can hang** on the configured
  credential store or CLI hooks. If `docker pull` produces no output, retry
  with a `DOCKER_CONFIG` directory whose `config.json` sets only the current
  context (no `credsStore`). Public images need no credentials.

## Parse DAGs and install requirements

```bash
docker run --rm --user airflow --entrypoint /bin/bash \
  -v "$PWD/dags:/validate/dags:ro" \
  -v "$PWD/requirements.txt:/validate/requirements.txt:ro" \
  -e MWAA__DB__POSTGRES_HOST=127.0.0.1 -e MWAA__DB__POSTGRES_PORT=5432 \
  -e MWAA__DB__POSTGRES_DB=airflow -e MWAA__DB__POSTGRES_SSLMODE=disable \
  -e MWAA__DB__POSTGRES_USER=airflow -e MWAA__DB__POSTGRES_PASSWORD=placeholder \
  -e AIRFLOW__CORE__LOAD_EXAMPLES=False \
  amazon-mwaa-docker-images/airflow:<target-version>-base -lc '
    pip install --quiet --user -r /validate/requirements.txt && echo REQUIREMENTS_OK
    python3 - <<"PY"
import warnings
warnings.simplefilter("default")  # print user-code deprecations the scheduler hides
try:
    from airflow.dag_processing.dagbag import DagBag  # Airflow 3.x location
except ImportError:
    from airflow.models.dagbag import DagBag  # Airflow 2.x
try:
    bag = DagBag(dag_folder="/validate/dags", include_examples=False)
except TypeError:  # Airflow 3.3 removed include_examples
    bag = DagBag(dag_folder="/validate/dags")
print("DAGS_OK:", sorted(bag.dag_ids))
for path, err in bag.import_errors.items():
    print("IMPORT_ERROR:", path, "->", err.strip().splitlines()[-1])
PY'
```

- **Run as `airflow`.** Airflow is installed in the `airflow` user's
  site-packages; as `root`, `import airflow` fails.
- **The placeholder `MWAA__DB__*` variables are required.** The image's
  `airflow_local_settings` refuses to import Airflow without them. Nothing
  listens on them, so a DAG that touches the metadata database at parse time
  (for example a module-level `Variable.get`) logs a connection error — treat
  that as a finding in itself.
- The placeholders are not credentials; never mount real connection strings.

## Interpret the result

| Output | Meaning | Action |
|---|---|---|
| `IMPORT_ERROR: ... ModuleNotFoundError: No module named 'imp'` | Standard-library removal on the new Python line | Fix in Phase 5 (for `imp`, use `importlib`) before the jump |
| `IMPORT_ERROR: ... cannot import name 'days_ago'` | AIR301 removal | Must fix before deploying to the target |
| `RemovedInAirflow3Warning` lines from `/validate/dags/...` | Code that works now but is removed in the next major | Fix in Phase 5; MWAA logs will not show these |
| `REQUIREMENTS_OK` missing | pip resolution failed against the constraints | Fix pins before `create-environment` / `update-environment` |

For a `python_change` jump, also run the parse on the **unchanged** source DAGs:
the difference between the source and target runs is the set of breaks the
upgrade introduces.
