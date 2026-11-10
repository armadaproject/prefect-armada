# prefect-armada

`prefect-armada` runs Prefect flow runs on [Armada](https://armadaproject.io/), a
multi-cluster batch scheduler for Kubernetes.

## Getting started

### Prerequisites

- An Armada server you can reach over gRPC.
- An Armada queue that your credentials are permitted to submit to.

### Creating a test Armada server with armada-operator

If you do not have access to a full Armada cluster, an Armada test cluster can be quickly set up
by using the [armada-operator](https://github.com/armadaproject/armada-operator/) - a Kubernetes
Operator for quickly setting up an Armada cluster on any Linux or macOS system that has
Docker installed. Typically, you can just clone that repository, then run `make kind-all` to
have it instantiate a basic K8s (`kind`) cluster and deploy Armada on top of it.

### Installation

You can use `prefect-armada` by either installing a PIP package, or by building and running from this source
repository.

Install `prefect-armada` with `pip`:

```bash
pip install prefect-armada
```

### Running the Prefect Server locally

Build the Prefect UI
In a copy of the [Prefect repo](https://github.com/PrefectHQ/prefect), build the UI:
```
$ uv run prefect dev build-ui --include-v2
```

Start the Prefect Server
```
$ PREFECT_UI_API_URL="http://EXTERNAL-IP-ADDRESS:4200/api" uv run prefect server start \
		--host 0.0.0.0 --port 4200 --analytics-off
```
If it prompts you for a profile, you can just press <RETURN> to accept the default `ephemeral` profile, and
accept the default PREFECT_API_URL value.

### Creating and Running an Armada work pool

First, create an Armada Work Pool. In your copy of the `prefect-armada` repo, run:
```
$ uv run prefect work-pool create --name my-armada-pool --type armada
```

Edit the settings for the work pool for your local Armada cluster. First, dump
the settings to a JSON file:
```
$ uv run prefect work-pool get-default-base-job-template --type armada > armada-base-job-template.py
```
Edit the file and change settings as needed (here we use Vim, but any text editor is fine):
```
$ vim armada-base-job-template.json
```
Note that some settings are quoted strings, some are (unquoted) boolean
values, and some are integer values. For each setting sub-clause, search for the `default` key and change its value to the appropriate default value.  If using the armada-operator to set up an Armada cluster, the settings should be:

- `armada_host`: "<IP address of your Armada cluster>"
- `armada_port`: 30002
- `armada_disable_ssl`: true
- `api_dns_name`: "172.18.0.1"

Save the file and update the work pool definition with it:
```
$ uv run prefect work-pool update --name my-armada-pool --base-job-template armada-base-job-template.py
```

View the work-pool's settings and confirm they are correct:
```
$ uv run prefect work-pool inspect --name my-armada-pool | less
```
If all looks good, start the pool worker:
```
$ uv run prefect worker start --pool "my-armada-pool"
```
If it complains that the Armada queue it is using (e.g. `prefect`) does not
exist, you can create it by running `armadactl create queue prefect`; then try
starting the worker again.

### Submitting a Test Job

An example test job is in `examples/hello_armada.py`. To run it:
```
$ cd examples
$ uv run hello_armada.py
$ uv run prefect deployment run 'hello/hello-armada'
```
You should be able to see the Flow run in the Prefect Server web UI.

### Submitting jobs from a flow

`ArmadaJob` submits an arbitrary Armada job and waits for it to finish:

```python
from prefect import flow
from prefect_armada import ArmadaJob
from prefect_armada.credentials import ArmadaCredentials


@flow
def run_a_job():
    job = ArmadaJob(
        credentials=ArmadaCredentials(),
        queue="prefect",
        job_request={
            "priority": 1,
            "podSpec": {
                "restartPolicy": "Never",
                "containers": [
                    {
                        "name": "prefect-job",
                        "image": "docker.io/library/ubuntu:latest",
                        "args": ["echo", "hello from armada"],
                        "resources": {
                            "requests": {"cpu": "120m", "memory": "510Mi"},
                            "limits": {"cpu": "120m", "memory": "510Mi"},
                        },
                    }
                ],
            },
        },
    )

    job_run = job.trigger()
    job_run.wait_for_completion()
    return job_run.fetch_result()
```

Individual Armada API calls are also available as tasks in
`prefect_armada.jobs`, `prefect_armada.jobsets`, `prefect_armada.queues`,
`prefect_armada.events`, and `prefect_armada.logs`.

## How it works

Armada is asynchronous: the worker submits a job and Armada schedules it onto one
of its Kubernetes clusters later. The worker therefore returns as soon as Armada
accepts the job, and an **observer** thread watches the job set's event stream to

- replicate Armada job events (`prefect.armada.job.*`) into Prefect's event
  system for use in Automations, and
- mark a flow run as `Crashed` when its Armada job fails before the flow run can
  report its own state, forwarding the job's logs to the flow run.

Each flow run is submitted to its own job set by default, so the observer's watch
ends when the flow run's job finishes. The observer can be disabled with
`PREFECT_INTEGRATIONS_ARMADA_OBSERVER_ENABLED=false`, and job sets submitted by
other processes can be watched with
`PREFECT_INTEGRATIONS_ARMADA_OBSERVER_JOB_SETS="my-queue/my-job-set"`.

### Feedback

If you encounter any bugs while using `prefect-armada`, feel free to open an
issue in the [prefect](https://github.com/armadaproject/prefect-armada) repository.

### Contributing

If you'd like to help contribute to fix an issue or add a feature to
`prefect-armada`, please
[propose changes through a pull request from a fork of the repository](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork).
