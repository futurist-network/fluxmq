#!/usr/bin/env python3
"""Summarize a folded CPU profile (`GET /api/debug/pprof/profile?format=folded`).

Usage: pprof-top.py PROFILE.folded [-n 25] [--grep REGEX]

Prints the functions with the most samples on top of the stack (self) and anywhere on the
stack (total), as a share of all samples. `--grep` keeps only stacks containing REGEX.
"""
import argparse
import re
import sys
from collections import Counter


def short(name: str) -> str:
    """`<a::B<T> as c::D>::f::<X>` -> `a::B as c::D>::f`: generics dropped, path kept."""
    out, depth = [], 0
    for ch in name:
        if ch == "<":
            depth += 1
            if depth > 1:
                continue
        elif ch == ">":
            depth -= 1
            if depth > 0:
                continue
        elif depth > 1:
            continue
        out.append(ch)
    s = "".join(out).replace("<>", "")
    return re.sub(r"::h[0-9a-f]{16}$", "", s)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("profile")
    ap.add_argument("-n", type=int, default=25)
    ap.add_argument("--grep")
    a = ap.parse_args()
    pat = re.compile(a.grep) if a.grep else None
    self_c, total_c, threads = Counter(), Counter(), Counter()
    all_n = 0
    with open(a.profile, encoding="utf-8", errors="replace") as f:
        for line in f:
            stack, _, n = line.rstrip("\n").rpartition(" ")
            if not stack or not n.isdigit():
                continue
            if pat and not pat.search(stack):
                continue
            n = int(n)
            thread, *frames = stack.split(";")
            # Drop the sampler's own frames (older builds kept them on top of every stack).
            frames = [short(x) for x in frames
                      if "pprof::" not in x and not x.startswith("backtrace::")] or ["?"]
            all_n += n
            threads[re.sub(r"[-_]?\d+$", "", thread)] += n
            self_c[frames[-1]] += n
            for fr in set(frames):
                total_c[fr] += n
    if not all_n:
        sys.exit("no samples")
    print(f"samples: {all_n}")
    print("\nthreads:")
    for t, n in threads.most_common(8):
        print(f"  {100 * n / all_n:5.1f}%  {t}")
    for title, c in (("self", self_c), ("total", total_c)):
        print(f"\ntop {title}:")
        for fn, n in c.most_common(a.n):
            print(f"  {100 * n / all_n:5.1f}%  {fn[:150]}")


if __name__ == "__main__":
    main()
