#!/usr/bin/env python3
import importlib.util
import pathlib
import types
import sys

mw_path = pathlib.Path(__file__).resolve().parents[1] / "biocypher_metta" / "metta_writer.py"

mod_bio = types.ModuleType('biocypher')
mod_logger = types.ModuleType('biocypher._logger')
setattr(mod_logger, 'logger', type('L', (), {
    'info': lambda *a, **k: None,
    'debug': lambda *a, **k: None,
    'warning': lambda *a, **k: None,
    'error': lambda *a, **k: None
})())
sys.modules['biocypher'] = mod_bio
sys.modules['biocypher._logger'] = mod_logger
sys.modules['networkx'] = types.ModuleType('networkx')
mod_pkg = types.ModuleType('biocypher_metta')
class BaseWriter:
    def __init__(self, *a, **k):
        pass
setattr(mod_pkg, 'BaseWriter', BaseWriter)
sys.modules['biocypher_metta'] = mod_pkg
spec = importlib.util.spec_from_file_location("metta_writer", str(mw_path))
mw = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mw)
def check_property(val):
    return mw.MeTTaWriter.check_property(None, val)

cases = [
    "https://www.bgee.org/download?geneId=ENSG00000123456",
    "ftp://ftp.example.com/path/file.txt",
    "www.example.com/path",
    "Hello World!",
    "a/b?c=d&e=f",
    "http://insecure.example.com/path"
]

for s in cases:
    out = check_property(s)
    print(f"ORIG: {s}")
    print(f"OUT:  {out}")
    print("---")
