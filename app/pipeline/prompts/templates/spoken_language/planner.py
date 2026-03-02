"""Spoken language planner prompt."""

PLANNER = """\
## Spoken Language Analysis Tools
- propose_wordlist: Generate a concept wordlist / gloss list by searching the internet/LLM knowledge.
  params: {{topic: str, constraints?: dict}}
- collect_multilingual_rows: Harvest multilingual data.
  params: {{wordlist: list[str], scope?: dict}}
- read_csv: Parse CSV and preview structure.
  params: {{csv_data: str}}
- normalize_spoken_language_csv: Repair and normalize CSV formatting.
  params: {{csv_data: str}}
- to_binary_matrix: Convert CSV to binary matrix.
  params: {{csv_data: str}}
- cluster: Cluster languages via HDBSCAN.
  params: {{csv_data: str, params?: dict}}
"""
