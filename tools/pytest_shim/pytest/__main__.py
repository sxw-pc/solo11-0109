"""Test runner used when real pytest is unavailable in the offline sandbox.

Usage mirrors the project command: ``python -m pytest -q`` from the repo
root. ``src`` is placed on the path exactly as pyproject's pythonpath does.
"""
import importlib.util,pathlib,sys,traceback

ROOT=pathlib.Path(__file__).resolve().parents[3]
sys.path.insert(0,str(ROOT/'src'))
sys.path.insert(0,str(ROOT/'tools'/'pytest_shim'))
import pytest  # the local shim

def load(path):
    name='t_'+path.stem
    spec=importlib.util.spec_from_file_location(name,path)
    mod=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

def main(argv):
    quiet='-q' in argv
    paths=[pathlib.Path(a) for a in argv[1:] if not a.startswith('-')]
    files=paths or sorted((ROOT/'tests').glob('test_*.py'))
    total_pass=total_fail=0;all_failures=[]
    for f in files:
        try:
            mod=load(f)
        except Exception:
            total_fail+=1;all_failures.append((str(f),'collection error',traceback.format_exc()));continue
        p,fl,fails=pytest.run_module(mod)
        total_pass+=p;total_fail+=fl
        for item in fails:all_failures.append((f'{f}::{item[0]}',item[1],item[2]))
    if not quiet:
        for name,msg,tb in all_failures:
            print(f'FAILED {name}: {msg}')
            if tb:print(tb)
    summary=f'{total_pass} passed' + (f', {total_fail} failed' if total_fail else '')
    print(summary,'in shim mode')
    return 1 if total_fail else 0

if __name__=='__main__':
    sys.exit(main(sys.argv))
