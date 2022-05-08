#!/usr/bin/env python3
# -*- coding: utf-8 -*-


"""Run Invoke tasks."""

# pylint: disable=invalid-name,broad-except,unused-argument


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
    """Load e vironment variables from .env, if available."""
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
    prefect_storage_config_name="",
    tags="aws-create",
):
    """Run an Ansible playbook."""
    # Run playbook to manage cloud storage for Prefect
    cmd = (
        "ANSIBLE_STDOUT_CALLBACK=yaml "
        "ANSIBLE_LOCALHOST_WARNING=false "
        "ansible-playbook -i hosts configure_prefect.yml "
        f"-e ansible_python_interpreter='{py_interpreter_path}' "
    )
    # Run playbook to
    # # (a) configure local env to use Prefect Cloud as API server
    # # (b) configure Prefect storage or re-use (set) default Prefect storage
    if tags == "configure":
        cmd += (
            f"-e prefect_storage_config_name='{prefect_storage_config_name}' "
        )
    run(f"{cmd}--tags {tags}")
    # Show available Prefect storage (including storage set as default)
    if tags == "configure":
        run("prefect storage ls")


@task(pre=[load_env_vars])
def start_jupyterlab(ctx):
    """Start Jupyter Lab."""
    run("jupyter lab")


@task(pre=[load_env_vars])
def run_nbs(ctx):
    """Run notebooks programmatically."""
    run("python3 papermill_runner.py")


@task(pre=[load_env_vars])
def run_pipe(ctx):
    """Run data pipeline from standalone script."""
    run("python3 run_data_pipe.py")


@task
def start_workflow(
    ctx,
    py_interpreter_path,
    prefect_storage_config_name="",
    tags="aws-create",
    action="jupyterlab",
):
    """Run workflow."""
    # Set local env to use Prefect Cloud and configure/set Prefect storage
    run_ansible_pb(
        ctx,
        py_interpreter_path=py_interpreter_path,
        prefect_storage_config_name=prefect_storage_config_name,
        tags=tags,
    )
    # Run (a) Jupyter Lab, (b) notebooks or (c) data pipeline script
    if action == "jupyterlab":
        start_jupyterlab(ctx)
    elif action == "run_nbs":
        run_nbs(ctx)
    else:
        run_pipe(ctx)
