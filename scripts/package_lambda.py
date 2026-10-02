"""Package src/lakehouse into build/lambda.zip (POSIX paths, no __pycache__)."""
import os
import zipfile

SRC = "src"
OUT = os.path.join("build", "lambda.zip")

os.makedirs("build", exist_ok=True)
with zipfile.ZipFile(OUT, "w", zipfile.ZIP_DEFLATED) as zf:
    for root, dirs, files in os.walk(os.path.join(SRC, "lakehouse")):
        dirs[:] = [d for d in dirs if d != "__pycache__"]
        for name in files:
            if name.endswith(".py"):
                path = os.path.join(root, name)
                zf.write(path, os.path.relpath(path, SRC).replace(os.sep, "/"))
print("built", OUT)