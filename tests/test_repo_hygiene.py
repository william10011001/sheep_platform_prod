import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_gitignore_is_clean_utf8_and_covers_core_artifacts():
    data = (ROOT / ".gitignore").read_bytes()
    assert b"\x00" not in data

    text = data.decode("utf-8")
    assert "app/dist/" in text
    assert "app/build/" in text
    assert "app/OpenNode.spec" in text

    ignored_paths = [
        "app/dist/example.txt",
        "app/build/example.txt",
        "app/OpenNode.spec",
        "app/__pycache__/example.pyc",
        "Factor_Dependency_Report/example.csv",
        "example.log",
    ]
    for rel_path in ignored_paths:
        result = subprocess.run(
            ["git", "check-ignore", rel_path],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, f"{rel_path} should be ignored"
