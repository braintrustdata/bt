class _FakeState:
    def login(self, **kwargs):
        return self


_STATE = _FakeState()


def _internal_get_global_state():
    return _STATE


# Imported by the runner at module load; only reached for a cross-org source.
def login_to_state(org_name=None):
    return _STATE
