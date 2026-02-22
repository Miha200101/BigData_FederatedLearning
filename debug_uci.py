"""
debug_uci.py
------------
Diagnosticare fișiere UCI - rulează din radacina proiectului:
  python debug_uci.py
"""
import os
from pathlib import Path

RAW_PATH = "data/raw/uci/"

for fname in ["student-mat.csv", "student-por.csv"]:
    fpath = RAW_PATH + fname
    print(f"\n{'='*60}")
    print(f"Fisier: {fpath}")
    
    if not os.path.exists(fpath):
        print("  LIPSA!")
        continue
    
    size = Path(fpath).stat().st_size
    print(f"  Marime: {size} bytes ({size/1024:.1f} KB)")
    
    # Citeste raw primele 300 bytes
    with open(fpath, "rb") as f:
        raw = f.read(300)
    print(f"  Primii bytes (hex): {raw[:50].hex()}")
    print(f"  Primii bytes (repr): {repr(raw[:200])}")
    
    # Detecteaza separator
    sep_count = {";": raw.count(b";"), ",": raw.count(b","), "\t": raw.count(b"\t")}
    print(f"  Separatori gasiti: {sep_count}")
    
    # Incearca sa citeasca cu pandas
    import pandas as pd
    for enc in ["latin-1", "cp1252", "utf-8", "iso-8859-1"]:
        for sep in [";", ",", "\t"]:
            try:
                df = pd.read_csv(fpath, sep=sep, encoding=enc, nrows=5)
                print(f"\n  OK cu encoding={enc}, sep={repr(sep)}")
                print(f"  Coloane ({len(df.columns)}): {list(df.columns)}")
                print(f"  Prima linie: {df.iloc[0].to_dict()}")
                break
            except Exception as e:
                pass
        else:
            continue
        break
    else:
        print("  TOATE combinatiile au esuat!")

print("\nGATA!")
