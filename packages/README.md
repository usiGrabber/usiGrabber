To create a new package create a new folder inside `packages` and run `uv init --package`.

Install a package from the same workspace: `uv add --workspace test-package-1`. You can then import it:

```python
from test_package import main as test_package_main

def main() -> None:
    print("Hello from usigrabber!")
    test_package_main()

if __name__ == "__main__":
    main()
```

Add an empty file called `py.typed` file into src/{package_name} folder to get type hints on build

More infos: https://docs.astral.sh/uv/concepts/projects/workspaces/#workspace-sources
