import os


def is_env_variable_true(env_var_name: str) -> bool:
    """Check if an environment variable is set to '1'."""
    return os.environ.get(env_var_name, "0").strip() == "1"
