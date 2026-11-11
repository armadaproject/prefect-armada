# Image for `hello_no_git_deploy.py`: a published Prefect image with the flow's
# source baked in, so flow-run pods need no git checkout or remote storage.
#
# The deployment's entrypoint is resolved relative to WORKDIR, which is where
# `prefect flow-run execute` starts in the pod.
FROM prefecthq/prefect:3-latest

WORKDIR /opt/prefect/flows
COPY hello_armada.py .
