import re
import json
import unicodedata
from dataclasses import dataclass
from typing import Dict, Iterable, Optional, Tuple

# Regex patterns
URL_RE = re.compile(r"\bhttps?://[^\s<>()\]]+|\bwww\.[^\s<>()\]]+", re.IGNORECASE)
MENTION_RE = re.compile(r"(?<!\w)@([A-Za-z0-9-]{1,39})\b")  # GitHub username max length = 39
ISSUE_REF_RE = re.compile(r"(?:(?<=\s)|^)(?:[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)?#\d+\b")
COMMIT_RE = re.compile(r"\b[a-f0-9]{7,40}\b", re.IGNORECASE)

# Emoji and symbol removal
EMOJI_RE = re.compile(
    "["
    "\U0001F300-\U0001F9FF"  # emoticons, symbols, pictographs, transport, etc.
    "\U0001F600-\U0001F64F"  # emoticons
    "\U0001F680-\U0001F6FF"  # transport & map symbols
    "\U0001F700-\U0001F77F"  # alchemical symbols
    "\U0001F780-\U0001F7FF"  # Geometric Shapes Extended
    "\U0001F800-\U0001F8FF"  # Supplemental Arrows-C
    "\U0001F900-\U0001F9FF"  # Supplemental Symbols and Pictographs
    "\U0001FA00-\U0001FA6F"  # Chess Symbols
    "\U0001FA70-\U0001FAFF"  # Symbols and Pictographs Extended-A
    "\U00002702-\U000027B0"  # Dingbats (includes ❓)
    "\U000024C2-\U0001F251"  # Enclosed characters
    "\U0001F900-\U0001F9FF"  # Supplemental Symbols
    "\U00002600-\U000027BF"  # Miscellaneous Symbols
    "]+",
    flags=re.UNICODE
)

# Markdown code
FENCED_CODE_RE = re.compile(r"```.*?```", re.DOTALL)
INLINE_CODE_RE = re.compile(r"`([^`\n]+)`")

# Paths/flags/versions (heuristics)
FILEPATH_RE = re.compile(r"\b(?:[A-Za-z]:\\|/)?(?:[\w.\- ]+/)+[\w.\- ]+\b")
FLAG_RE = re.compile(r"(?<!\w)--?[A-Za-z][\w\-]*(?:=[^\s]+)?")
VERSION_RE = re.compile(r"\bv?\d+(?:\.\d+){1,3}\b", re.IGNORECASE)

# Terminal/progress/escape-ish noise
ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
UNICODE_REPLACEMENT_CHAR_RE = re.compile(r"\uFFFD")
PROGRESS_BAR_RE = re.compile(r"\b(?:\d{1,3}%\|.*?\|\s*\d+/\d+)", re.DOTALL)  # tqdm-like
ITERATION_SPAM_RE = re.compile(r"(?:^|\n)\s*Iteration:\s*\d+%?\|.*?(?:\n|$)", re.IGNORECASE)

# Markdown links/images
MD_LINK_RE = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")
MD_IMAGE_RE = re.compile(r"!\[([^\]]*)\]\(([^)]+)\)")
BLOCKQUOTE_RE = re.compile(r"(?m)^\s*>\s?.*$")
HTML_TAG_RE = re.compile(r"<[^>]+>")

WHITESPACE_RE = re.compile(r"[ \t]+")
MANY_NEWLINES_RE = re.compile(r"\n{3,}")

@dataclass
class CleanConfig:
    replace_codeblocks: bool = True
    replace_inline_code: bool = True
    replace_urls: bool = True
    replace_mentions: bool = True
    replace_issue_refs: bool = True
    replace_commits: bool = True
    replace_paths: bool = True
    replace_flags: bool = True
    replace_versions: bool = True

    drop_blockquotes: bool = True  # set False to keep but tag
    drop_html: bool = True
    keep_md_link_text: bool = True  # markdown structure of "[text](url)" becomes "text" (URL is handled separately)

    # Noise handling
    drop_if_noise_ratio_ge: float = 0.1 # 0.0 ~ 1.5 (1.5 would be extremely weak at catching) recommended 0.1-0.3
    drop_if_too_long: int = 15000   # optional hard cap (chars)
    compress_progress_spam: bool = True

def normalize_text(s: str) -> str:
    s = unicodedata.normalize("NFKC", s)
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = ANSI_ESCAPE_RE.sub("", s)
    s = UNICODE_REPLACEMENT_CHAR_RE.sub("", s)  # removes �
    s = EMOJI_RE.sub(" ", s)
    # remove control chars (keep \n, \t)
    s = "".join(ch for ch in s if (ch == "\n" or ch == "\t" or (ord(ch) >= 32 and ord(ch) != 127)))
    return s

def is_mostly_noise(s: str) -> Tuple[bool, float, Dict[str, int]]:
    """
    Heuristic: if a large portion of the text is made of non-letter tokens, progress bars, or repeated iteration lines.
    """
    if not s:
        return True, 1.0, {"len": 0}

    length = len(s)
    letters = sum(ch.isalpha() for ch in s)
    digits = sum(ch.isdigit() for ch in s)
    spaces = sum(ch.isspace() for ch in s)
    other = length - letters - digits - spaces

    progress_hits = len(PROGRESS_BAR_RE.findall(s))
    iter_lines = len(ITERATION_SPAM_RE.findall(s))

    # noise_score is effectively: proportion of non-letter chars + penalty for progress/iteration spam
    # This catches common noise in the GitHub data like test outputs, terminal dumps, logs, etc.
    noise_score = (other / max(1, length)) + min(1.0, (progress_hits + iter_lines) / 10.0) * 0.5

    meta = {
        "len": length,
        "letters": letters,
        "digits": digits,
        "spaces": spaces,
        "other": other,
        "progress_hits": progress_hits,
        "iter_lines": iter_lines,
    }
    return noise_score, meta

def clean_github_text(text: str, cfg: CleanConfig) -> Tuple[Optional[str], Dict]:
    """
    Returns (cleaned_text or None if dropped, metadata)
    Implements the pipeline steps you described.
    """
    meta: Dict = {}
    if text is None:
        return None, {"dropped": True, "reason": "None"}

    text = normalize_text(text)

    if cfg.drop_if_too_long and len(text) > cfg.drop_if_too_long:
        meta["truncated_from"] = len(text)
        text = text[:cfg.drop_if_too_long]

    if cfg.compress_progress_spam:
        text = ITERATION_SPAM_RE.sub("\n[PROGRESS]\n", text)
        text = re.sub(r"(?:\n\[PROGRESS\]\n){3,}", "\n[PROGRESS]\n", text)

    if cfg.drop_blockquotes:
        text = BLOCKQUOTE_RE.sub("", text)

    if cfg.drop_html:
        text = HTML_TAG_RE.sub(" ", text)

    text = MD_IMAGE_RE.sub(" IMAGE ", text)

    if cfg.keep_md_link_text:
        text = MD_LINK_RE.sub(r"\1", text)

    if cfg.replace_codeblocks:
        text = FENCED_CODE_RE.sub(" CODEBLOCK ", text)

    if cfg.replace_inline_code:
        text = INLINE_CODE_RE.sub(" INLINECODE ", text)

    if cfg.replace_urls:
        text = URL_RE.sub(" URL ", text)

    if cfg.replace_mentions:
        text = MENTION_RE.sub(" USER ", text)

    if cfg.replace_issue_refs:
        text = ISSUE_REF_RE.sub(" ISSUE_REF ", text)

    if cfg.replace_commits:
        text = COMMIT_RE.sub(" COMMIT ", text)

    if cfg.replace_paths:
        text = FILEPATH_RE.sub(" FILEPATH ", text)

    if cfg.replace_flags:
        text = FLAG_RE.sub(" FLAG ", text)

    if cfg.replace_versions:
        text = VERSION_RE.sub(" VERSION ", text)

    text = WHITESPACE_RE.sub(" ", text)
    text = MANY_NEWLINES_RE.sub("\n\n", text)
    text = text.strip()

    if len(text) < 5:
        return None, {"dropped": True, "reason": "too_short"}

    noise_score, noise_meta = is_mostly_noise(text)
    meta.update({"noise_score": noise_score, **noise_meta})
    if noise_score >= cfg.drop_if_noise_ratio_ge:
        return None, {"dropped": True, "reason": "mostly_noise", **meta}
    
    meta["dropped"] = False
    return text, meta

def read_jsonl(path: str) -> Iterable[dict]:
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            yield obj

def write_jsonl(path: str, rows: Iterable[dict]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def process_jsonl(in_path: str, out_path: str, cfg: CleanConfig = CleanConfig()) -> None:
    kept = 0
    dropped = 0
    out_rows = []

    meta_dict = []

    for row in read_jsonl(in_path):
        raw = row['text']
        # row is a string (from JSON)
        cleaned, meta = clean_github_text(raw, cfg)
        if cleaned is None:
            dropped += 1
        else:
            kept += 1
            out_rows.append({"id": row.get("id", ""), "text": cleaned})

        meta["text"] = row

        meta_dict.append(meta)

    write_jsonl(out_path + ".meta.jsonl", meta_dict)
    write_jsonl(out_path, out_rows)
    print(f"Done. Kept={kept}, Dropped={dropped}, Total={kept+dropped}. Output: {out_path}")


def main():
    process_jsonl(
        "data/processed/texts_only_with_ids.jsonl",
        "data/processed/texts_only_with_ids_cleaned.jsonl"
    )

if __name__ == "__main__":
    main()
