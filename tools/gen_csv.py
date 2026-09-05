"""Generate a test CSV campaign file (columns: to,text,sender).

Generated files are NOT committed (see .gitignore). Streams output so it can
produce very large files (millions of rows) in bounded memory.

    python tools/gen_csv.py --count 5000000 --out campaign_5m.csv
    python tools/gen_csv.py --count 100000 --bad-every 1000   # inject bad rows
"""

from __future__ import annotations

import argparse
import csv
import sys


def main() -> None:
    parser = argparse.ArgumentParser(description="generate a test CSV of SMS rows")
    parser.add_argument("--count", type=int, default=1_000_000)
    parser.add_argument("--out", default="campaign.csv")
    parser.add_argument("--text", default="Hello from relay campaign, message number")
    parser.add_argument(
        "--bad-every", type=int, default=0, help="emit an invalid row every N rows (0 = none)"
    )
    args = parser.parse_args()

    with open(args.out, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["to", "text", "sender"])
        for i in range(args.count):
            if args.bad_every and i % args.bad_every == 0 and i > 0:
                writer.writerow(["", f"{args.text} {i}", "RELAY"])  # missing 'to' -> skipped
                continue
            # vary the destination a little so it is not a single number
            msisdn = f"+4071{i % 10_000_000:07d}"
            writer.writerow([msisdn, f"{args.text} {i}", "RELAY"])
            if i % 500_000 == 0 and i > 0:
                print(f"generated {i} rows...", file=sys.stderr, flush=True)

    print(f"wrote {args.count} rows to {args.out}", file=sys.stderr)


if __name__ == "__main__":
    main()
