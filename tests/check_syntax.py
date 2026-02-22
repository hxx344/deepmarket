import ast, sys

files = [
    "src/dashboard/ws_handler.py",
]

ok = True
for f in files:
    try:
        with open(f, encoding="utf-8") as fh:
            ast.parse(fh.read())
        print(f"OK: {f}")
    except SyntaxError as e:
        print(f"FAIL: {f} -> {e}")
        ok = False

sys.exit(0 if ok else 1)
