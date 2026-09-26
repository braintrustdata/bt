class LocalTrace:
    def __init__(
        self,
        object_type=None,
        object_id=None,
        root_span_id=None,
        ensure_spans_flushed=None,
        state=None,
    ):
        self.root_span_id = root_span_id

    def get_configuration(self):
        return {"root_span_id": self.root_span_id}

    async def get_spans(self, include_scorers=False):
        return [
            {
                "id": "source-row",
                "span_id": "source-span",
                "input": {"prompt": "hello"},
                "output": {"answer": "world"},
                "expected": "ok",
                "metadata": {"topic": "smoke"},
            }
        ]
