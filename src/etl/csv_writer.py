import csv
from typing import Any, Iterable, Optional

def write_csv(
    rows: Iterable[dict[str, Any]],
    filepath: str = "output.csv",
    fieldnames: Optional[list[str]] = None,
) -> int:
    it = iter(rows)
    first = next(it, None)
    if first is None:
        open(filepath, "w", encoding="utf-8").close()
        return 0

    if fieldnames is None:
        fieldnames = list(first.keys())

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()

        count = 0
        writer.writerow(first)
        count += 1

        for row in it:
            writer.writerow(row)
            count += 1

    return count