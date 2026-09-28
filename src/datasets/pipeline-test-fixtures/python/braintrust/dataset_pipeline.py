_DATASET_PIPELINES = []


def DatasetPipeline(name=None, source=None, target=None, transform=None):
    pipeline = {
        "name": name,
        "source": dict(source or {}),
        "target": dict(target or {}),
        "transform": transform,
    }
    _DATASET_PIPELINES.append(pipeline)
    return pipeline
