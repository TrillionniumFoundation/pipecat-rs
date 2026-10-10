"""Developer-only oracle recorder; Rust tests consume checked-in JSON.

Run with sentencex==1.0.32, Markdown, pydantic, and loguru installed.
Only package initialization is bypassed; production source and upstream tests
are imported unchanged. This is never invoked by Cargo.
"""
import asyncio
import importlib
import inspect
import hashlib
import importlib.metadata
import unicodedata
import json
from pathlib import Path
import sys
import types
import unittest

ROOT = Path(__file__).resolve().parents[4]
pkg = types.ModuleType("pipecat")
pkg.__path__ = [str(ROOT / "src/pipecat")]
sys.modules["pipecat"] = pkg
sys.path.insert(0, str(ROOT / "tests"))
from pipecat.utils.text.simple_text_aggregator import SimpleTextAggregator
from pipecat.utils.text.skip_tags_aggregator import SkipTagsAggregator
from pipecat.utils.text.pattern_pair_aggregator import PatternPairAggregator
from pipecat.utils.text.markdown_text_filter import MarkdownTextFilter

traces = []
utility_calls = []
current_test = ""
depth = 0

def serial(value):
    if hasattr(value, "text"):
        result = {"text": value.text, "type": str(value.type)}
        if hasattr(value, "full_match"):
            result["full_match"] = value.full_match
        return result
    if isinstance(value, (list, tuple)):
        return [serial(v) for v in value]
    if isinstance(value, dict):
        return {k: serial(v) for k, v in value.items()}
    if hasattr(value, "value"):
        return value.value
    return value

# Record direct utility calls made by the original utility test modules.
for mod_name, names in {
    "markup_utils": ["strip_markup", "strip_complete_markup", "raw_offset_after_clean_chars", "split_markup_runs"],
    "alnum_utils": ["alnum_only", "has_alnum", "strip_trailing_punctuation", "fold_typography", "fold_for_matching", "advance_by_chars", "advance_by_alnums"],
    "word_timestamp_utils": ["merge_punct_tokens"],
    "string": ["match_endofsentence", "resolve_sentence_tokenizer_language", "longest_trailing_partial_match", "concatenate_aggregated_text"],
}.items():
    module = importlib.import_module("pipecat.utils.string" if mod_name == "string" else "pipecat.utils.text." + mod_name)
    for name in names:
        original = getattr(module, name)
        def utility(*args, _original=original, _name=name, **kwargs):
            result = _original(*args, **kwargs)
            utility_calls.append({"test": current_test, "function": _name, "args": serial(args), "kwargs": serial(kwargs), "result": serial(result)})
            return result
        setattr(module, name, utility)

for cls in [SimpleTextAggregator, SkipTagsAggregator, PatternPairAggregator, MarkdownTextFilter]:
    original_init = cls.__init__
    def init(self, *args, _original=original_init, _cls=cls, **kwargs):
        global depth
        outer = depth == 0
        depth += 1
        try: _original(self, *args, **kwargs)
        finally: depth -= 1
        if not outer: return
        config = dict(kwargs)
        if args: config["tags"] = args[0]
        if "params" in config: config["params"] = config["params"].model_dump()
        self.__trace = {"test": current_test, "class": _cls.__name__, "config": serial(config), "operations": []}
        traces.append(self.__trace)
    cls.__init__ = init
    for method in ["aggregate", "flush", "reset", "handle_interruption", "set_language", "add_pattern", "filter", "update_settings", "reset_interruption", "remove_tables"]:
        if not hasattr(cls, method): continue
        original = getattr(cls, method)
        if inspect.isasyncgenfunction(original):
            async def wrapped(self, *args, _original=original, _method=method, **kwargs):
                global depth
                outer = depth == 0
                depth += 1
                before = getattr(self, "_text", None)
                try: result = [v async for v in _original(self, *args, **kwargs)]
                finally: depth -= 1
                if outer: self.__trace["operations"].append({"method": _method, "args": serial(args), "kwargs": serial(kwargs), "before": before, "result": serial(result)})
                for value in result: yield value
        elif inspect.iscoroutinefunction(original):
            async def wrapped(self, *args, _original=original, _method=method, **kwargs):
                global depth
                outer = depth == 0
                depth += 1
                before = getattr(self, "_text", None)
                try: result = await _original(self, *args, **kwargs)
                finally: depth -= 1
                if outer: self.__trace["operations"].append({"method": _method, "args": serial(args), "kwargs": serial(kwargs), "before": before, "result": serial(result)})
                return result
        else:
            def wrapped(self, *args, _original=original, _method=method, **kwargs):
                global depth
                outer = depth == 0
                depth += 1
                before = getattr(self, "_text", None)
                try: result = _original(self, *args, **kwargs)
                finally: depth -= 1
                if outer: self.__trace["operations"].append({"method": _method, "args": serial(args), "kwargs": serial(kwargs), "before": before, "result": serial(result) if result is not self else None})
                return result
        setattr(cls, method, wrapped)

class Result(unittest.TextTestResult):
    def startTest(self, test):
        global current_test
        current_test = test.id()
        super().startTest(test)

modules = ["test_simple_text_aggregator", "test_skip_tags_aggregator", "test_pattern_pair_aggregator", "test_markdown_text_filter", "test_markup_utils", "test_word_timestamp_utils"]
suite = unittest.TestSuite(unittest.defaultTestLoader.loadTestsFromName(m) for m in modules)
result = unittest.TextTestRunner(resultclass=Result).run(suite)
if not result.wasSuccessful(): raise SystemExit("Upstream tests failed; refusing to record golden data")
# Additional actual-source streaming code/table state and Unicode coverage.
async def extra():
    global current_test
    current_test = "additional.markdown_streaming_state"
    f = MarkdownTextFilter(params=MarkdownTextFilter.InputParams(filter_code=True))
    for s in ["Before ```python", "print('hello')", "``` after", "```", "hidden", "``` done"]: await f.filter(s)
    await f.handle_interruption()
    await f.filter("``` unfiltered interruption")
    await f.reset_interruption()
    await f.filter("normal")
    current_test = "additional.markdown_extended"
    f = MarkdownTextFilter()
    for s in ["<user@example.com>", "one\n1. two", "one\n- two", "[label](https://x.y)", "![alt](img.png)", "<https://example.com>", "- one\n- two", "a\n---", "first\n2. item", "**broken", "~~strike~~", "before ```x\ny``` after", "```python\nx = 1\n```", "`` &quot;quoted&quot; ``", "a &quot;x&quot;", "\ue000PC0\ue001", "say __strong__ and _emphasis_", "A [link **strong**](https://x.example/path)"]:
        await f.filter(s)
    current_test = "additional.html_table_streaming_state"
    f = MarkdownTextFilter(params=MarkdownTextFilter.InputParams(filter_tables=True))
    for s in ["before <table><tr>", "<td>x</td>", "</table> after", "x<table>all</table>y"]: f.remove_tables(s)
    await f.handle_interruption()
    f.remove_tables("<table>after interrupt")
    current_test = "additional.sentence_language_and_quotes"
    for language, text in [("en", "Albert I. Douglas is here. Hello."), ("en", 'She said, “Dr. Smith is here.” Then left.'), ("de-DE", "Das ist z.B. wichtig. Weiter geht es."), ("ja", "Dr. 田中が来ます。次の文です。"), ("en", "Visit https://example.com/path?q=1.2. Next."), ("en", "We make a good team, you and I. Did?")]:
        a = SimpleTextAggregator(language=language)
        for c in text: _ = [v async for v in a.aggregate(c)]
        await a.flush()
    from pipecat.utils.text import alnum_utils as a
    from pipecat.utils import string as st
    current_test = "additional.unicode_and_offsets"
    for text in ["Café déjà-vu!", "ＳＴＲＡＳＳＥ Straße 你好 한글", "Don’t ‘stop’—now", "İ Å É A\u0301", "<speak>你好, café!</speak>", "5 < 10", "<>hi", "Ⅻ½²", "ab<unclosed", "a…！？", "emoji😊"]:
        for function in [a.alnum_only, a.has_alnum, a.strip_trailing_punctuation, a.fold_typography, a.fold_for_matching]: function(text)
        for n in [0, 1, 3, 100]:
            a.advance_by_chars(text, 0, n)
            a.advance_by_alnums(text, 0, n)
    for language in [None, "", "auto", " DE_AT ", "pt-BR", "unknown", "ja"]: st.resolve_sentence_tokenizer_language(language)
    for text, language in [("👋 Hello. 😀 Next", "en"), ("Cafe\u0301 is open. Next", "en"), ("\n\nHello. Next", "en"), ("Hello.\n\nNext", "en"), ("こんにちは。次", "ja"), ("Dr. 田中が来ます。次の文です。", "ja"), ("U.S.のAWSです。次の文です。", "ja"), ("नमस्ते। यह", "hi"), ("هل أنت بخير؟ نعم", "ar")]: st.match_endofsentence(text, language=language)
asyncio.run(extra())
output = ROOT / "rust/crates/pipecat-text/tests/fixtures/source_traces.json"
output.write_text(json.dumps({"source_tests_run": result.testsRun, "dependencies": {n: importlib.metadata.version(n) for n in ["sentencex", "Markdown", "pydantic", "loguru"]}, "unicode_version": unicodedata.unidata_version, "source_sha256": {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted([*(ROOT / "src/pipecat/utils/text").glob("*.py"), ROOT / "src/pipecat/utils/string.py", *[ROOT / "tests" / (name + ".py") for name in modules]])}, "traces": traces, "utility_calls": utility_calls}, ensure_ascii=False, indent=2) + "\n")
print(f"Wrote {len(traces)} traces from {result.testsRun} passing original tests to {output}")
