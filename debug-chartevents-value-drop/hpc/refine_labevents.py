#!/usr/bin/env python3
"""Split the labevents residue into de-identification placeholder vs real text.

`___` is MIMIC's redaction marker. Rows carrying it have no information to
lose, so counting them as ETL data loss would overstate the case badly.
"""
import argparse, json
from pathlib import Path
import duckdb

CMP_NUM = r'^\s*(<=|>=|<|>|=)?\s*-?[0-9]*\.?[0-9]+\s*$'

p = argparse.ArgumentParser()
p.add_argument('--oracle-db', type=Path, required=True)
p.add_argument('--out-dir', type=Path, required=True)
a = p.parse_args()
a.out_dir.mkdir(parents=True, exist_ok=True)

con = duckdb.connect()
con.execute("SET threads=16"); con.execute("SET memory_limit='96GB'")
con.execute(f"ATTACH '{a.oracle_db}' AS oracle (READ_ONLY)")
con.execute(f"""CREATE VIEW resid AS
SELECT itemid, value, valuenum FROM oracle.mimiciv_hosp.labevents
WHERE valuenum IS NOT NULL AND value IS NOT NULL
  AND NOT regexp_matches(value, '[a-zA-Z]')
  AND NOT regexp_matches(value, '[^\\x00-\\x7F]')
  AND NOT regexp_matches(value, '{CMP_NUM}')""")

out = {}
out['residue_total'] = con.execute("SELECT count(*) FROM resid").fetchone()[0]
out['residue_deid_placeholder'] = con.execute(
    "SELECT count(*) FROM resid WHERE value LIKE '%___%'").fetchone()[0]
out['residue_real_text'] = con.execute(
    "SELECT count(*) FROM resid WHERE value NOT LIKE '%___%'").fetchone()[0]
out['residue_real_examples'] = [
    {'itemid': i, 'label': l, 'value': v, 'n_rows': n,
     'valuenum_min': str(lo), 'valuenum_max': str(hi)}
    for i, l, v, n, lo, hi in con.execute("""
        SELECT r.itemid, max(d.label), r.value, count(*) n,
               min(r.valuenum), max(r.valuenum)
        FROM resid r LEFT JOIN oracle.mimiciv_hosp.d_labitems d USING (itemid)
        WHERE r.value NOT LIKE '%___%'
        GROUP BY r.itemid, r.value ORDER BY n DESC LIMIT 20""").fetchall()]
out['ambiguous_pairs_excluding_deid'] = con.execute("""
    SELECT count(*) FROM (
        SELECT itemid, valuenum FROM resid WHERE value NOT LIKE '%___%'
        GROUP BY itemid, valuenum HAVING count(DISTINCT value) > 1)""").fetchone()[0]

(a.out_dir / 'labevents_refined.json').write_text(json.dumps(out, indent=2) + '\n')
print(json.dumps(out, indent=2))
