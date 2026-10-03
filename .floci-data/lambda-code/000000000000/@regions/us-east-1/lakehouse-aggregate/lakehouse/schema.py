"""Canonical schema and tolerant parsing for ANP well-production CSVs.

ANP files differ in column names (e.g. "Poço" vs "Nome Poço ANP", "Período" vs "Mês/Ano")
and in units. Add aliases here after inspecting the header of the file you download.
"""
import re
import unicodedata

ALIASES = {
    "basin": {"bacia"},
    "field": {"campo"},
    "well": {"poco", "nome_poco_anp"},
    "period": {"periodo", "mes_ano"},
    "oil_volume": {"oleo_bbl_dia", "producao_de_oleo_m3", "oleo"},
}
REQUIRED = ("well", "period", "oil_volume")
NUMERIC = ("oil_volume",)

_LOOKUP = {alias: canonical for canonical, names in ALIASES.items() for alias in names}


def normalize_key(name: str) -> str:
    text = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


def canonicalize(raw: dict) -> dict:
    out = {}
    for key, value in raw.items():
        canonical = _LOOKUP.get(normalize_key(key or ""))
        if canonical and canonical not in out:
            out[canonical] = value.strip() if isinstance(value, str) else value
    return out


def raw_key_for(row: dict, canonical: str):
    for key in row:
        if _LOOKUP.get(normalize_key(key or "")) == canonical:
            return key
    return None


def to_float(value):
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if "," in text and "." in text:
        text = text.replace(".", "").replace(",", ".")
    elif "," in text:
        text = text.replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None
