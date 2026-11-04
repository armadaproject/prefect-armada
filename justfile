test:
    uv run pytest

# Generate API reference documentation for prefect-armada. Source links point
# at this repository's `origin` remote.
api-ref:
    uvx --with-editable . \
        --python 3.12 \
        --isolated \
        'mdxify>=0.2.45' \
        --all \
        --root-module prefect_armada \
        --output-dir docs/api-ref \
        --anchor-name "prefect-armada-sdk-reference" \
        --include-inheritance
