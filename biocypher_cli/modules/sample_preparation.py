"""Sample preparation utilities for BioCypher CLI"""
import gzip
import logging
import os
import subprocess
import tempfile
import yaml
from pathlib import Path
from typing import Dict, List, Optional, Union
from urllib.parse import urlparse

from rich.console import Console
from rich.panel import Panel

PROJECT_ROOT = Path(__file__).parent.parent.parent

logger = logging.getLogger(__name__)
console = Console()

def _detect_file_format(path: Path) -> str:
    """Detect file format by examining filename and content."""
    try:
        filename = path.name.lower()

        # Check filename patterns first
        if "rnacentral_rfam_annotations" in filename:
            return "rnacentral_rfam"
        if any(filename.endswith(ext) for ext in ['.gff', '.gtf', '.gff3']):
            return "gff"
        if any(ext in filename for ext in ['.bed', '.narrowpeak', '.broadpeak']):
            return "bed"

        is_gz = path.suffix == ".gz"
        opener = gzip.open if is_gz else open

        with opener(path, "rt", encoding="utf-8", errors="ignore") as f:
            lines = []
            for i, line in enumerate(f):
                if i >= 10:  # Check first 10 lines
                    break
                lines.append(line.strip())

        if not lines:
            return "unknown"

        # GFF/GTF detection (has tab-separated columns with standard fields)
        tab_lines = [line for line in lines if "\t" in line and not line.startswith("#")]
        if tab_lines:
            parts = tab_lines[0].split("\t")
            if len(parts) >= 8 and parts[0] and parts[3].isdigit() and parts[4].isdigit():
                return "gff"

        # BED detection (tab-separated with numeric start/end columns)
        if tab_lines:
            parts = tab_lines[0].split("\t")
            if len(parts) >= 3 and parts[1].isdigit() and parts[2].isdigit():
                return "bed"

        # Default to tabular for other tab-separated files
        if any("\t" in line for line in lines):
            return "tabular"

        return "unknown"

    except Exception:
        return "unknown"

def _normalize_file_format(path: Path) -> None:
    """Automatically detect and normalize common file format issues for sample files.

    Handles various file format transformations based on filename patterns and content:
    - RNAcentral RFAM annotations (9-column to 3-column)
    - GFF/GTF files (.gff, .gtf)
    - TSV/CSV files with inconsistent formatting
    - BED files (.bed)
    - Gzipped and plain text files

    Operates in-place on plain or gzipped files.
    """

    try:
        file_format = _detect_file_format(path)

        if file_format == "rnacentral_rfam":
            _normalize_rnacentral_rfam(path)
        elif file_format == "gff":
            _normalize_gff_file(path)
        elif file_format in ["bed", "tabular"]:
            _normalize_tabular_file(path)
        
    except Exception:
        return

def _normalize_rnacentral_rfam(path: Path) -> None:
    """Convert RNAcentral RFAM annotations from 9-column to 3-column format."""
    _normalize_three_column(path, lambda parts: _extract_rfam_columns(parts))

def _normalize_tabular_file(path: Path) -> None:
    """General tabular file normalization - ensure consistent formatting."""
    def processor(line: str):
        # Skip completely empty lines
        if not line.strip():
            return None
        # Basic cleanup: ensure consistent tab separation, remove trailing whitespace
        parts = line.split("\t")
        cleaned_line = "\t".join(part.strip() for part in parts)
        return cleaned_line
    
    _transform_file_in_place(path, processor)

def _normalize_gff_file(path: Path) -> None:
    """Normalize GFF/GTF files - ensure proper format."""
    def processor(line: str):
        if not line or line.startswith("#"):
            return line
        # GFF/GTF should have at least 8 tab-separated columns
        parts = line.split("\t")
        if len(parts) >= 8:
            # Ensure proper formatting
            return line
        return None  
    
    _transform_file_in_place(path, processor)

def _transform_file_in_place(path: Path, line_processor) -> None:
    """Generic in-place file transformation using a line processor function.
    
    line_processor should be a callable that takes a line (str) and returns 
    the transformed line(s) as a list of strings, or None to skip the line.
    """
    try:
        is_gz = path.suffix == ".gz"
        opener_in = gzip.open if is_gz else open
        opener_out = gzip.open if is_gz else open

        with tempfile.NamedTemporaryFile("wb", delete=False, dir=str(path.parent), suffix=path.suffix if is_gz else "") as tmp:
            tmp_path = Path(tmp.name)
            with opener_in(path, "rt", encoding="utf-8", errors="replace") as src, \
                 opener_out(tmp_path, "wt", encoding="utf-8", errors="replace") as out:

                for line in src:
                    result = line_processor(line.rstrip("\n"))
                    if result is not None:
                        if isinstance(result, list):
                            for r in result:
                                out.write(r + "\n")
                        else:
                            out.write(result + "\n")

        path.unlink(missing_ok=True)
        tmp_path.replace(path)
    except Exception:
        return

def _normalize_three_column(path: Path, column_extractor) -> None:
    """Generic 3-column file normalizer using a column extraction function."""
    def processor(line: str):
        parts = line.split("\t")
        if not parts or parts[0].startswith("#"):
            return None
        result = column_extractor(parts)
        if result and len(result) == 3:
            return "\t".join(result)
        return None
    
    _transform_file_in_place(path, processor)

def _extract_columns_by_patterns(parts: list, patterns: list) -> list:
    """Generic column extractor that finds values matching given patterns.
    
    patterns: list of tuples (column_index, pattern_prefix, default_value, format_func)
    Returns list of extracted values, or None if extraction fails.
    """
    if len(parts) < len(patterns):
        return None
    
    result = []
    for i, (col_idx, prefix, default, format_func) in enumerate(patterns):
        value = ""
        if col_idx < len(parts):
            part = parts[col_idx]
            if part.startswith(prefix):
                value = part
        if not value and default and col_idx < len(parts):
            value = parts[col_idx]
        if not value:
            return None
        if format_func:
            value = format_func(value)
        result.append(value)
    
    return result if len(result) == len(patterns) else None

def _extract_rfam_columns(parts: list) -> list:
    """Extract columns for RNAcentral RFAM format: rna_id, go_term, rfam."""
    patterns = [
        (0, "", "", None),  # rna_id from column 0, no prefix
        (-1, "GO:", "", None),  # go_term from any column starting with GO:
        (-1, "RF", "", lambda x: x if x.startswith("Rfam:") else f"Rfam:{x}")  # rfam from any column starting with RF, format as Rfam:
    ]
    return _extract_columns_by_patterns(parts, patterns)

def find_url_for_filename(config_path: str, filename: str) -> Optional[str]:
    """
    Search all URLs in the data_source_config YAML for a URL ending with the given filename.
    Case-insensitive compare so sample suffix casing mismatches do not break lookup.
    Handles URLs as strings, lists, or dicts.
    """
    config_path = Path(config_path)
    if not config_path.exists():
        return None

    raw_name = filename.strip()

    def _variants(name: str) -> set:
        name_l = name.lower()
        variants = {name_l, Path(name_l).name, Path(name_l).stem}
        if name_l.startswith("sample_"):
            variants.add(name_l[len("sample_"):])
        for suffix in ("_sample", "-sample"):
            if suffix in name_l:
                variants.add(name_l.replace(suffix, ""))
        if "_" in name_l:
            parts = name_l.split("_", 1)
            if len(parts) == 2 and parts[1]:
                variants.add(parts[1])
        return {v.strip() for v in variants if v}

    needles = _variants(raw_name)

    def _matches(url: str) -> bool:
        from urllib.parse import urlparse

        url_l = url.strip().lower()
        url_name = Path(urlparse(url_l).path).name
        url_variants = _variants(url_name) | {url_name}
        return bool(needles & url_variants) or any(url_l.endswith(n) for n in needles)

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    for entry in config.values():
        urls = entry.get("url")
        if not urls:
            continue
        if isinstance(urls, str):
            if _matches(urls):
                return urls.strip()
        elif isinstance(urls, list):
            for u in urls:
                if isinstance(u, str) and _matches(u):
                    return u.strip()
                elif isinstance(u, dict):
                    for suburl in u.values():
                        if isinstance(suburl, str) and _matches(suburl):
                            return suburl.strip()
        elif isinstance(urls, dict):
            for suburl in urls.values():
                if isinstance(suburl, str) and _matches(suburl):
                    return suburl.strip()
                elif isinstance(suburl, list):
                    for u in suburl:
                        if isinstance(u, str) and _matches(u):
                            return u.strip()
    return None

def check_and_prepare_samples(adapters_config_path: str, selected_adapters: List[str] = None, limit: int = 10000
) -> List[str]:
    """Ensure sample files referenced by the adapters config exist.
    If a sample is missing, attempt to find a download URL in `config/data_source_config.yaml`
    and run `scripts/download_and_sample.py` to create a sampled file. Returns list of created files.
    """
    created = []
    try:
        config_path = Path(adapters_config_path)
        if not config_path.exists():
            console.print(f"[yellow]Adapters config not found: {config_path}[/]")
            return created
        with open(config_path) as f:
            adapters = yaml.safe_load(f) or {}
    except Exception as e:
        console.print(f"[red]Failed to read adapters config: {e}[/]")
        return created

    data_source = {}
    try:
        ds_path = PROJECT_ROOT / "config" / "data_source_config.yaml"
        if ds_path.exists():
            with open(ds_path) as f:
                data_source = yaml.safe_load(f) or {}
    except Exception:
        data_source = {}

    def _candidate_names(name: str) -> List[str]:
        names = set()
        cur = name.lower()
        names.add(cur)

        # Add a variant that strips common sample suffixes but keeps extensions (e.g., foo_sample.csv.gz -> foo.csv.gz)
        for suffix in ("_sample", "-sample"):
            if cur.endswith(suffix):
                parts = cur.split(".")
                if len(parts) > 1:
                    stem = ".".join(parts[:-1])
                    ext = parts[-1]
                    if stem.endswith(suffix):
                        names.add(stem[: -len(suffix)] + "." + ext)


        for marker in ("_sample.", "-sample."):
            if marker in cur:
                names.add(cur.replace(marker, "."))

        def strip_ext(val: str) -> str:
            while True:
                base, ext = os.path.splitext(val)
                if ext.lower() in {".gz", ".gzip", ".bz2", ".zip", ".tsv", ".csv", ".txt", ".bed", ".gtf", ".gff", ".vcf"}:
                    val = base
                else:
                    break
            return val

        stripped = strip_ext(cur)
        names.add(stripped)
        names.add(Path(stripped).stem)

        for variant in list(names):
            if variant.endswith("_sample"):
                names.add(variant[: -len("_sample")])
            if variant.endswith("-sample"):
                names.add(variant[: -len("-sample")])

        for candidate in list(names):
            if candidate.startswith("sample_"):
                names.add(candidate[len("sample_"):])

        return [n for n in names if n]

    def _extract_url(entry, basename: str = None, prefer_dir: bool = False) -> Optional[str]:
        """Normalize url extraction across dict/list/str entries with optional basename hints."""
        if not entry:
            return None

        url_field = entry.get("url") if isinstance(entry, dict) else entry

        def _pick_from_dict(url_dict: Dict[str, str]) -> Optional[str]:
            if prefer_dir:
                for key in ("pwm", "annotation"):
                    if key in url_dict:
                        return url_dict.get(key)

            if isinstance(basename, str):
                lower_name = basename.lower()
                for k, v in url_dict.items():
                    if isinstance(k, str) and k.lower() in lower_name:
                        return v

            vals = list(url_dict.values())
            return vals[0] if vals else None

        if isinstance(url_field, list):
            return url_field[0] if url_field else None
        if isinstance(url_field, dict):
            return _pick_from_dict(url_field)
        if isinstance(url_field, str):
            return url_field
        return None

    def _is_file_sane(path: Path) -> (bool, str):
        """Level-1 file sanity checks: size>0 and at least one non-blank line.
        Supports plain text and gzip (.gz) files.
        Returns (True, "") when sane, or (False, reason).
        """
        try:
            if not path.exists():
                return False, "file missing"

            if path.is_dir():
                for child in path.rglob('*'):
                    if child.is_file():
                        sane, reason = _is_file_sane(child)
                        if sane:
                            return True, "directory contains at least one sane file"
                return False, "directory contains no sane files"

            size = path.stat().st_size
            if size == 0:
                return False, "file size is 0 bytes"

            open_fn = gzip.open if path.suffix == ".gz" else open
            with open_fn(path, "rt", errors="ignore") as fh:
                for line in fh:
                    stripped = line.strip()
                    if not stripped:
                        continue
                    lower = stripped.lower()
                    if lower.startswith("<!doctype") or lower.startswith("<html"):
                        return False, "file looks like HTML"
                    return True, ""
            return False, "no non-blank lines found"
        except Exception as e:
            return False, f"exception while validating file: {e}"

    for name, conf in (adapters.items() if isinstance(adapters, dict) else []):
        if selected_adapters and name not in selected_adapters:
            continue
        args = {}
        try:
            args = conf.get("adapter", {}).get("args", {}) or {}
        except Exception:
            args = {}

        # Apply general file format normalization for all file paths
        file_fields = []
        for k, v in args.items():
            if isinstance(v, str) and ("samples" in v or k.lower().find("file") != -1 or k.lower().find("path") != -1):
                file_fields.append(v)
                file_path = Path(v)
                if not file_path.is_absolute():
                    file_path = PROJECT_ROOT / file_path
                if file_path.exists():
                    _normalize_file_format(file_path)

        for fp in file_fields:
            out_path = Path(fp)
            if not out_path.is_absolute():
                out_path = PROJECT_ROOT / out_path

            looks_like_dir = out_path.suffix == "" or str(fp).endswith(os.sep)

            if out_path.exists():
                sane, reason = _is_file_sane(out_path)
                if sane:
                    continue
                else:
                    console.print(f"[yellow]Existing sample {out_path} failed sanity check: {reason}. Will attempt to recreate.[/]")

            basename = Path(fp).name
            def _lookup_url(basename: str, adapter_name: str, looks_like_dir: bool) -> Optional[str]:
                cfg_path = str(PROJECT_ROOT / "config" / "data_source_config.yaml")

                # Prefer exact adapter-specific entry before heuristic filename matching.
                entry = data_source.get(adapter_name) or data_source.get(adapter_name.lower())
                if not entry:
                    # Check for substring matches with adapter_name
                    for ds_key, ds_entry in data_source.items():
                        if ds_key in adapter_name or adapter_name in ds_key:
                            entry = ds_entry
                            break
                if entry:
                    if looks_like_dir:
                        url_local = _extract_url(entry, basename=basename, prefer_dir=True)
                        if url_local:
                            return url_local

                    url_local = _extract_url(entry, basename=basename)
                    if url_local:
                        return url_local

                url_local = find_url_for_filename(cfg_path, basename)
                if url_local:
                    return url_local

                for cand in _candidate_names(basename):
                    cand_no_ext = Path(cand).stem
                    url_local = find_url_for_filename(cfg_path, cand)
                    if url_local:
                        return url_local
                    for ds_key, entry in data_source.items():
                        key_norm = ds_key.lower()
                        if (
                            key_norm == cand_no_ext
                            or key_norm == cand
                            or cand.startswith(key_norm)
                            or key_norm.startswith(cand)
                            or key_norm in cand
                            or cand in key_norm
                        ):
                            url_local = _extract_url(entry, basename=basename, prefer_dir=looks_like_dir)
                            if url_local:
                                return url_local

                return None

            url = _lookup_url(basename, name, looks_like_dir)

            if not url and looks_like_dir:
                # Directory target with no URL even after lookup; ensure it exists and skip download
                out_path.mkdir(parents=True, exist_ok=True)
                continue

            if not url:
                console.print(Panel.fit(f"[yellow]Sample for adapter '{name}' is missing: {out_path}[/]"))
                from questionary import text
                inp = text(f"Enter input URL or local path to download for '{name}' (leave blank to skip):")
                val = inp.unsafe_ask()
                if not val:
                    console.print(f"[dim]Skipping adapter {name} (no input provided).[/]")
                    continue
                url = val

            # run download_and_sample.py
            cmd = ["python3", str(PROJECT_ROOT / "scripts" / "download_and_sample.py"), "--input", str(url), "--output", str(out_path), "--limit", str(limit)]
            console.print(Panel.fit(f"[blue]Preparing sample for '{name}' by running download_and_sample.py[/]"))
            try:
                proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, cwd=str(PROJECT_ROOT))
                for line in proc.stdout:
                    console.print(f"[dim]{line.rstrip()}[/]")
                proc.wait()

                def _handle_result(returncode, out_path, attempt_url):
                    if returncode == 0 and out_path.exists():
                        sane, reason = _is_file_sane(out_path)
                        if sane:
                            console.print(f"[green]Wrote sample file: {out_path}[/]")
                            created.append(str(out_path))
                            return True
                        else:
                            console.print(f"[red]Sample created but failed sanity check ({reason}): {out_path}[/]")
                            return False
                    else:
                        console.print(f"[red]Failed to create sample for adapter {name} using {attempt_url}[/]")
                        return False

                success = _handle_result(proc.returncode, out_path, url)
                if not success:
                   
                    alt_inp = text(f"Enter alternate input URL or local path for '{name}' (leave blank to skip):")
                    alt_val = alt_inp.unsafe_ask()
                    if not alt_val:
                        console.print(f"[dim]Skipping adapter {name} (no alternative provided).[/]")
                    else:
                        retry_cmd = ["python3", str(PROJECT_ROOT / "scripts" / "download_and_sample.py"), "--input", str(alt_val), "--output", str(out_path), "--limit", str(limit)]
                        console.print(Panel.fit(f"[blue]Retrying sample preparation for '{name}' with provided input[/]"))
                        try:
                            proc2 = subprocess.Popen(retry_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, cwd=str(PROJECT_ROOT))
                            for line in proc2.stdout:
                                console.print(f"[dim]{line.rstrip()}[/]")
                            proc2.wait()
                            _handle_result(proc2.returncode, out_path, alt_val)
                        except Exception as e:
                            console.print(f"[red]Retry failed for {name}: {e}[/]")
            except Exception as e:
                console.print(f"[red]Error running download_and_sample for {name}: {e}[/]")

    return created