#!/usr/bin/env python3
"""Model-family normalisation.

Lanstar model codes carry the length and colour in the suffix: LS-6UTPD-2MG is the 2m
green variant of LS-6UTPD. Certification and spec documents apply to the whole family,
so a request for LS-6UTPD-3MG must resolve to the same documents as LS-6UTPD-2MG.

Two traps this must avoid:
  * connector gender, not colour — LS-RGB-15MM (15-pin Male-Male), LS-SER-9MF
  * a digit+M that is part of the name — LS-U81MH is not "LS-U8, 1m, H"
So a candidate base only becomes a family when the data actually shows sibling variants
(or the supplier wrote an explicit wildcard such as LS-6UTPD-XXM).
"""
import re
from collections import defaultdict

GENDER = {'M', 'F', 'MM', 'MF', 'FM', 'FF', 'MMF', 'FFM', 'MFF'}
WILD = re.compile(r'^(?:X{1,2}|\*)$')

# length preceded by a separator:  LS-6UTPD-2MG , LS-6STPD-0.5M , LS-6UTPD-XXM
SEP = re.compile(r'^(?P<base>.+?)[-_ ](?P<len>\d+(?:\.\d+)?|X{1,2}|\*)\s*M(?P<col>[A-Z]{0,3})$')
# length glued to the name:        LS-FT6U2M , LS-HDMI-HYB-R10M , LS-7SD-BK1M
NOSEP = re.compile(r'^(?P<base>.*[A-Z])(?P<len>\d+(?:\.\d+)?)M(?P<col>[A-Z]{0,3})$')


def candidate(model):
    """(base, length, colour) if the tail looks like a length/colour suffix, else None."""
    m = (model or '').upper().strip()
    for rx in (SEP, NOSEP):
        x = rx.match(m)
        if not x:
            continue
        col = x.group('col')
        if col in GENDER:          # 15MM / 9MF = connector gender, not a variant
            continue
        base = x.group('base').rstrip('-_ .')
        if len(base) < 5:
            continue
        return base, x.group('len'), col
    return None


def build(models):
    """Group a model list into families. Returns (family_of, members_of)."""
    cand = {}
    groups = defaultdict(set)
    for m in models:
        c = candidate(m)
        if c:
            cand[m] = c[0]
            groups[c[0]].add(m)

    families = set()
    for base, members in groups.items():
        # a real family: more than one variant, or the base itself is a known model,
        # or the supplier used an explicit wildcard variant
        if len(members) > 1 or base in models or any(
                WILD.match(candidate(x)[1]) for x in members if candidate(x)):
            families.add(base)

    family_of, members_of = {}, defaultdict(set)
    for m in models:
        f = cand.get(m)
        f = f if f in families else m
        family_of[m] = f
        members_of[f].add(m)
    # a base that exists as its own model belongs to its family too
    for f in list(members_of):
        if f in family_of and family_of[f] != f:
            pass
    return family_of, {k: sorted(v) for k, v in members_of.items()}


def resolve(query, families):
    """Map an arbitrary user-typed model to a known family key.
    families: iterable of family keys produced by build()."""
    q = (query or '').upper().strip()
    fam = set(families)
    if q in fam:
        return q, 'exact'
    c = candidate(q)
    if c and c[0] in fam:
        return c[0], 'suffix'                      # LS-6UTPD-3MG -> LS-6UTPD
    # longest known family that the query starts with
    pref = [f for f in fam if q.startswith(f) and len(q) > len(f)]
    if pref:
        return max(pref, key=len), 'prefix'
    # the reverse: the user typed a shorter stem than the stored key
    # ("LS-LKPG" -> "LS-LKPG SERIES", "LS-6SFTP" -> "LS-6SFTP-300M")
    rev = [f for f in fam if f.startswith(q) and len(q) >= 6
           and (len(f) == len(q) or not f[len(q)].isalnum() or f[len(q)].isdigit())]
    if len(rev) == 1:
        return rev[0], 'stem'
    if rev:                       # ambiguous stem: caller should ask which one
        return sorted(rev, key=len)[0], 'stem_ambiguous'
    return None, 'none'
