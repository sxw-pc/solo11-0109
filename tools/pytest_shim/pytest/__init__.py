"""Minimal offline stand-in for the parts of pytest this repository uses.

The real project test suite targets pytest. This sandbox has no network and
no pytest/pip available, so this shim implements just the surface the suite
needs (``raises``, ``tmp_path`` fixture, plain function discovery) using the
standard library only. It is never imported by application code.
"""
import inspect,pathlib,re,tempfile,traceback

class Failed(Exception):
    """Raised by assertion helpers; mapped to a failed test by the runner."""

class _Raises:
    def __init__(self,exc,match=None):
        self.exc=exc;self.match=match;self.value=None
    def __enter__(self):
        return self
    def __exit__(self,exc_type,exc,tb):
        if exc is None:
            raise Failed(f'DID NOT RAISE {self.exc.__name__}')
        if not issubclass(exc_type,self.exc):
            return False
        if self.match is not None and not re.search(self.match,str(exc)):
            raise Failed(f'pattern {self.match!r} not found in {str(exc)!r}')
        self.value=exc
        return True

def raises(exc,match=None):
    return _Raises(exc,match)

def fail(msg='failure'):
    raise Failed(msg)

def _fixtures(func):
    sig=inspect.signature(func)
    kwargs={}
    if 'tmp_path' in sig.parameters:
        d=tempfile.mkdtemp(prefix='pytest-')
        kwargs['tmp_path']=pathlib.Path(d)
    return kwargs

def run_module(mod):
    passed=failed=0;failures=[]
    for name,obj in vars(mod).items():
        if not name.startswith('test_') or not callable(obj):continue
        try:
            obj(**_fixtures(obj))
            passed+=1
        except Failed as exc:
            failed+=1;failures.append((name,str(exc),None))
        except Exception:
            failed+=1;failures.append((name,'unexpected exception',traceback.format_exc()))
    return passed,failed,failures
