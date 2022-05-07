#!/usr/bin/env python3
# -*- coding: utf-8 -*-


"""Rune Invoke tasks."""

# pylint: disable=invalid-name,broad-except


from dotenv import find_dotenv, load_dotenv
from invoke import task
from invoke.context import Context


def run(command: str, hide: bool = False):
    """Execute a command with Invoke."""
    ctx = Context()
    r = ctx.run(command, echo=True, pty=True, hide=hide)
    return r


@task
def load_env_vars(ctx):
    """Load environment variables from .env, if available."""
    try:
        load_dotenv(find_dotenv())
        print("Loaded environment variables from .env file")
    except Exception as e:
        print(
            f"Could not load environment variables from .env file. "
            f"Got error message: {str(e)}"
        )


@task(pre=[load_env_vars])
def run_ansible_pb(
    ctx,
    py_interpreter_path,
    prefect_storage_config_name="my-storage-1",
    tags="aws-create",
):
    """Run an Ansible playbook."""
    cmd = (
        "ANSIBLE_STDOUT_CALLBACK=yaml "
        "ANSIBLE_LOCALHOST_WARNING=false "
        "ansible-playbook -i hosts configure_prefect.yml "
        f"-e ansible_python_interpreter='{py_interpreter_path}' "
    )
    if tags == "configure":
        cmd += (
            f"-e prefect_storage_config_name='{prefect_storage_config_name}' "
        )
    run(f"{cmd}--tags {tags}")


@task(pre=[load_env_vars])
def start_jupyterlab(ctx):
    """Start Jupyter Lab."""
    run("jupyter lab")


@task(pre=[load_env_vars])
def run_nbs(ctx):
    """Run notebooks programmatically."""
    run("python3 papermill_runner.py")
