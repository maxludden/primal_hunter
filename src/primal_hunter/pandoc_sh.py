# primal_hunter/pandoc_sh.py
"""
A thin convenience wrapper around the `pandoc` CLI using the `sh` library.

Provides small, well-typed helpers for common operations:
- run a generic pandoc invocation
- convert strings or files between formats
- produce PDFs and EPUBs
- merge multiple documents
- query pandoc for supported formats and version

This module deliberately keeps a small surface area and returns either
captured output (str) or Path for file output operations.
"""
from __future__ import annotations

import logging
import os
import sys
import tempfile
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Union, cast

import sh
from sh import Command, ErrorReturnCode

from primal_hunter.logger import get_console, get_logger, get_progress




logger = logging.getLogger(__name__)


class PandocError(RuntimeError):
    """Raised when pandoc fails or when invocation is invalid."""


class Pandoc:
    """Helper to run pandoc via sh.Command.

    Basic usage:
        p = Pandoc()
        html = p.convert_string("# Hi", from_format="markdown", to_format="html")
        p.to_pdf("chapter.md", Path("chapter.pdf"))
    """

    def __init__(self, pandoc_cmd: str = "pandoc", max_workers: int = 4) -> None:
        self._cmd: Command = sh.Command(pandoc_cmd)
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._executor_shutdown = False

    def close(self) -> None:
        """Release resources held by the internal ThreadPoolExecutor."""
        if not self._executor_shutdown:
            self._executor.shutdown(wait=True)
            self._executor_shutdown = True

    def __enter__(self) -> "Pandoc":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def __del__(self) -> None:  # pragma: no cover - best effort cleanup
        try:
            self.close()
        except Exception:  # pragma: no cover - avoid errors during GC
            logger.debug(
                "Failed to close Pandoc executor during __del__", exc_info=True
            )

    # -------------------------
    # Introspection utilities
    # -------------------------
    def version(self) -> str:
        """Return pandoc's --version output (first line typically contains version)."""
        try:
            out = self._cmd("--version")
            return str(out).strip()
        except ErrorReturnCode as e:
            raise PandocError(
                f"pandoc --version failed: {e.stderr.decode() if e.stderr else e}"
            ) from e

    def list_input_formats(self) -> List[str]:
        """Return list of input formats supported by pandoc."""
        try:
            out = self._cmd("--list-input-formats")
            return [line.strip() for line in str(out).splitlines() if line.strip()]
        except ErrorReturnCode as e:
            raise PandocError(
                f"listing input formats failed: {e.stderr.decode() if e.stderr else e}"
            ) from e

    def list_output_formats(self) -> List[str]:
        """Return list of output formats supported by pandoc."""
        try:
            out = self._cmd("--list-output-formats")
            return [line.strip() for line in str(out).splitlines() if line.strip()]
        except ErrorReturnCode as e:
            raise PandocError(
                f"listing output formats failed: {e.stderr.decode() if e.stderr else e}"
            ) from e

    # -------------------------
    # Low level runner
    # -------------------------
    def run(
        self,
        inputs: Optional[Union[str, bytes, Path, Sequence[Path]]] = None,
        *,
        from_format: Optional[str] = None,
        to_format: Optional[str] = None,
        output: Optional[Union[str, Path]] = None,
        variables: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        latex_engine: Optional[str] = None,
        filters: Optional[Sequence[Union[str, Path]]] = None,
        lua_filters: Optional[Sequence[Union[str, Path]]] = None,
        extra_args: Optional[Sequence[str]] = None,
        capture_output: bool = True,
        standalone: bool = True,
        ok_return_codes: Optional[Sequence[int]] = None,
    ) -> str | Path:
        """Generic pandoc runner.

        - inputs may be:
            * a string/bytes -> will be passed to stdin
            * a Path -> passed as filename argument
            * a sequence of Path -> passed as multiple filename args
            * None -> pandoc will read from stdin (empty)

        - variables are passed as -V key=value
        - metadata is passed as --metadata=key:value
        - filters and lua_filters are passed using --filter or --lua-filter
        - extra_args are appended verbatim
        - if output is provided, returns Path(output). Otherwise returns captured stdout as str.
        - set capture_output=False to stream directly to stdout and receive an empty string.
        """
        args: List[str] = []

        if standalone:
            args.append("-s")

        if from_format:
            args.append(f"--from={from_format}")
        if to_format:
            args.append(f"--to={to_format}")

        if latex_engine:
            args.append(f"--pdf-engine={latex_engine}")

        if variables:
            for k, v in variables.items():
                args.extend(["-V", f"{k}={v}"])

        if metadata:
            for k, v in metadata.items():
                args.append(f"--metadata={k}={v}")

        if filters:
            for f in filters:
                args.append(f"--filter={str(f)}")
        if lua_filters:
            for f in lua_filters:
                args.append(f"--lua-filter={str(f)}")

        if extra_args:
            args.extend(list(extra_args))

        file_args: List[str] = []
        _in: Optional[Union[str, bytes]] = None
        if inputs is None:
            _in = ""
        elif isinstance(inputs, (str, bytes)):
            _in = inputs
        elif isinstance(inputs, Path):
            file_args.append(str(inputs))
        elif isinstance(inputs, (list, tuple)):
            for p in inputs:
                file_args.append(str(p))
        else:
            raise TypeError("inputs must be None, str/bytes, Path, or Sequence[Path]")

        ok_codes: Optional[Sequence[int]] = None
        if ok_return_codes is not None:
            ok_code_set = {0}
            ok_code_set.update(ok_return_codes)
            ok_codes = tuple(sorted(ok_code_set))

        cmd_kwargs: Dict[str, Any] = {"_in": _in, "_err_to_out": True}
        if ok_codes is not None:
            cmd_kwargs["_ok_code"] = ok_codes

        if not capture_output and output is None:
            cmd_kwargs["_out"] = sys.stdout

        if output:
            out_path = Path(output)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            args.extend(file_args)
            args.extend(["-o", str(out_path)])
            try:
                # when writing to a file, let pandoc produce it (no capture needed)
                self._cmd(*args, **cmd_kwargs)
                return out_path
            except ErrorReturnCode as e:
                raise PandocError(
                    f"pandoc failed: {e.stderr.decode() if e.stderr else e}"
                ) from e
        else:
            # capture output to return as string
            args.extend(file_args)
            try:
                out = self._cmd(*args, **cmd_kwargs)
                if capture_output:
                    return str(out)
                return ""
            except ErrorReturnCode as e:
                raise PandocError(
                    f"pandoc failed: {e.stderr.decode() if e.stderr else e}"
                ) from e

    # -------------------------
    # Conveniences
    # -------------------------
    def convert_string(
        self,
        src: str,
        *,
        from_format: str = "markdown",
        to_format: str = "html",
        variables: Optional[Dict[str, Any]] = None,
        extra_args: Optional[Sequence[str]] = None,
    ) -> str:
        """Convert a string and return the result as str."""
        return cast(
            str,
            self.run(
                src,
                from_format=from_format,
                to_format=to_format,
                variables=variables,
                extra_args=extra_args,
                capture_output=True,
            ),
        )

    def convert_file(
        self,
        src: Path,
        dst: Optional[Path] = None,
        *,
        from_format: Optional[str] = None,
        to_format: Optional[str] = None,
        variables: Optional[Dict[str, Any]] = None,
        extra_args: Optional[Sequence[str]] = None,
    ) -> Union[str, Path]:
        """Convert a single file. If dst is provided, returns Path(dst) otherwise str of stdout."""
        return self.run(
            src,
            from_format=from_format,
            to_format=to_format,
            output=dst,
            variables=variables,
            extra_args=extra_args,
        )

    def merge_files(
        self,
        inputs: Sequence[Path],
        output: Path,
        *,
        from_format: Optional[str] = None,
        to_format: Optional[str] = None,
        variables: Optional[Dict[str, Any]] = None,
        extra_args: Optional[Sequence[str]] = None,
    ) -> str|Path:
        """Concatenate multiple input files via pandoc and write to output."""
        if not inputs:
            raise ValueError("inputs must not be empty")
        return self.run(
            inputs,
            from_format=from_format,
            to_format=to_format,
            output=output,
            variables=variables,
            extra_args=extra_args,
        )

    def to_pdf(
        self,
        src: Union[Path, str],
        dst: Optional[Path] = None,
        *,
        latex_engine: str = "pdflatex",
        from_format: Optional[str] = None,
        extra_args: Optional[Sequence[str]] = None,
    ) -> str|Path:
        """Render markdown/HTML/etc to PDF using a specified LaTeX engine."""
        if isinstance(src, str):
            # treat as string input
            if dst is None:
                # create a temporary file
                fd, tmp = tempfile.mkstemp(suffix=".pdf")
                os.close(fd)
                dst = Path(tmp)
            return self.run(
                src,
                from_format=from_format,
                to_format="pdf",
                output=dst,
                latex_engine=latex_engine,
                extra_args=extra_args,
            )
        else:
            if dst is None:
                dst = src.with_suffix(".pdf")
            return self.run(
                src,
                from_format=from_format,
                to_format="pdf",
                output=dst,
                latex_engine=latex_engine,
                extra_args=extra_args,
            )

    def to_epub(
        self,
        src: Union[Path, str],
        dst: Optional[Path] = None,
        *,
        title: Optional[str] = None,
        author: Optional[str] = None,
        extra_args: Optional[Sequence[str]] = None,
    ) -> str|Path:
        """Convert input (file or string) to EPUB. Returns Path to created EPUB."""
        metadata = {}
        if title:
            metadata["title"] = title
        if author:
            metadata["author"] = author

        if isinstance(src, str):
            if dst is None:
                fd, tmp = tempfile.mkstemp(suffix=".epub")
                os.close(fd)
                dst = Path(tmp)
            return self.run(
                src,
                from_format=None,
                to_format="epub",
                output=dst,
                metadata=metadata or None,
                extra_args=extra_args,
            )
        else:
            if dst is None:
                dst = src.with_suffix(".epub")
            return self.run(
                src,
                from_format=None,
                to_format="epub",
                output=dst,
                metadata=metadata or None,
                extra_args=extra_args,
            )

    # -------------------------
    # Parallel helpers
    # -------------------------
    def convert_many(
        self,
        jobs: Iterable[Dict[str, Any]],
    ) -> List[Union[str, Path]]:
        """Run multiple conversions in parallel.

        Each job is a dict passed directly to self.run(...). Example:
            jobs = [
                {"inputs": Path("a.md"), "to_format": "html", "output": Path("a.html")},
                {"inputs": Path("b.md"), "to_format": "html", "output": Path("b.html")},
            ]
        """
        futures: List[Future] = []
        results: List[Union[str, Path]] = []
        for job in jobs:
            fut = self._executor.submit(self.run, **job)
            futures.append(fut)
        for fut in as_completed(futures):
            try:
                results.append(fut.result())
            except Exception:
                logger.exception("parallel pandoc job failed")
                raise
        return results

if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)

    sample_md = "# Demo\n\nThis is a small demo document produced by primal_hunter.pandoc_sh."

    with Pandoc() as p:
        try:
            # basic introspection
            print("pandoc:", p.version().splitlines()[0])
            print("input formats (sample):", p.list_input_formats()[:10])
            print("output formats (sample):", p.list_output_formats()[:10])

            # convert a string to HTML and show a short snippet
            html = p.convert_string(sample_md, from_format="markdown", to_format="html")
            print("\nHTML snippet:\n", html[:300].replace("\n", " ") + "...")

            # render string to temporary PDF and EPUB files
            pdf_tmp = Path(tempfile.mkstemp(suffix=".pdf")[1])
            epub_tmp = Path(tempfile.mkstemp(suffix=".epub")[1])
            pdf_path = p.to_pdf(sample_md, dst=pdf_tmp)
            epub_path = p.to_epub(sample_md, dst=epub_tmp, title="Demo", author="Demo Author")
            print("\nPDF written to:", pdf_path)
            print("EPUB written to:", epub_path)

            # demonstrate parallel conversions from temporary markdown files
            a = Path(tempfile.mkstemp(suffix=".md")[1])
            b = Path(tempfile.mkstemp(suffix=".md")[1])
            a.write_text("# A\n\nAlpha")
            b.write_text("# B\n\nBeta")
            jobs = [
                {"inputs": a, "to_format": "html", "output": a.with_suffix(".html")},
                {"inputs": b, "to_format": "html", "output": b.with_suffix(".html")},
            ]
            results = p.convert_many(jobs)
            print("\nParallel conversion results:", results)

        except PandocError as e:
            print("Pandoc failed:", e, file=sys.stderr)
            sys.exit(1)
