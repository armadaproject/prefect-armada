"""
An example flow that runs as an Armada job on a local kind dev cluster, with its
code directly baked into a Docker image instead of fetched from git.

Prerequisites:

  - An Armada server reachable over gRPC, with a queue the worker may submit to
    (`prefect` by default).
  - A Prefect server bound to an address the Armada cluster can route to, since
    the flow runs in a pod inside that cluster.
  - An `armada` work pool with a worker started against it.
  - Docker, and `kind` if the cluster is a kind cluster.

Then run:

    uv run hello_armada.py

This builds `Dockerfile` (a published Prefect image plus this file), loads the
image into the kind cluster's nodes so no registry is needed, and registers a
deployment that runs the flow from the image. Set KIND_CLUSTER to an empty
string to skip the load, e.g. when IMAGE names a registry the cluster pulls
from and PUSH is set. """

from __future__ import annotations

import os
import subprocess
from pathlib import Path

from prefect import flow, get_run_logger, task

WORK_POOL = os.environ.get("WORK_POOL", "my-armada-pool")
ARMADA_QUEUE = os.environ.get("ARMADA_QUEUE", "prefect")
IMAGE = os.environ.get("IMAGE", "prefect-armada-hello")
# Name of the kind cluster to load the image into; empty to skip.
KIND_CLUSTER = os.environ.get("KIND_CLUSTER", "armada")
# Push the image to IMAGE's registry after building it.
PUSH = os.environ.get("PUSH", "").lower() in ("1", "true", "yes")


@task
def greet(name: str) -> str:
    return f"Hello, {name}, from Armada!"


@flow(log_prints=True)
def hello(names: list[str] | None = None) -> list[str]:
    """Greet each name, one task run apiece."""
    logger = get_run_logger()
    greetings = [greet(name) for name in names or ["DefaultUser1", "DefaultUser2"]]
    for greeting in greetings:
        logger.warn(greeting)
    return greetings


if __name__ == "__main__":
    from prefect.docker import DockerImage

    # The directory is both the image's build context and the base the
    # deployment's entrypoint is recorded relative to, which must match the
    # image's WORKDIR layout: `hello_armada.py:hello`.
    os.chdir(Path(__file__).resolve().parent)

    image = DockerImage(name=IMAGE, dockerfile="Dockerfile")
    image.build()
    if PUSH:
        image.push()
    if KIND_CLUSTER:
        subprocess.run(
            ["kind", "load", "docker-image", image.reference, "--name", KIND_CLUSTER],
            check=True,
        )

    deployment_id = hello.deploy(
        name="hello-armada",
        work_pool_name=WORK_POOL,
        image=image,
        parameters={"names": ["Homer", "Marge", "Bart", "Lisa", "Maggie"]},
        job_variables={
            "queue": ARMADA_QUEUE,
            "namespace": "default",
            # A loaded image exists only on the cluster's nodes, so it must not
            # be pulled; each build gets a fresh tag, so reuse is never stale.
            "image_pull_policy": "Always" if PUSH else "IfNotPresent",
            # Armada rejects containers whose requests differ from their limits,
            # so keep each pair identical.
            "cpu_request": "500m",
            "cpu_limit": "500m",
            "memory_request": "512Mi",
            "memory_limit": "512Mi",
            # Lower values are scheduled first.
            "priority": 1,
        },
        # Already built, pushed, or loaded above.
        build=False,
        push=False,
    )

    print(f"Deployed {image.reference} as {deployment_id}")
    print("Run it with: uv run prefect deployment run 'hello/hello-armada'")
