import inspect


# The real SDK dispatches by signature; the fixture pipelines take **kwargs,
# where that dispatch is equivalent to forwarding every argument.
async def call_user_fn(loop, fn, **kwargs):
    result = fn(**kwargs)
    if inspect.isawaitable(result):
        return await result
    return result
