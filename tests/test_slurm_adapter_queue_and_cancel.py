import shlex


def test_build_squeue_job_query_command_contains_job_id_and_noheader():
    from dispatch.slurm_adapter import build_squeue_job_query_command

    cmd = build_squeue_job_query_command("12345")

    assert cmd[0] == "squeue"
    joined = " ".join(shlex.quote(x) for x in cmd)

    assert "--jobs=12345" in joined
    assert "--noheader" in cmd
    assert "--format=" in joined


def test_build_squeue_list_command_has_noheader_and_format():
    from dispatch.slurm_adapter import build_squeue_list_command

    cmd = build_squeue_list_command()

    assert cmd[0] == "squeue"
    assert "--noheader" in cmd
    assert any(x.startswith("--format=") for x in cmd)


def test_build_scancel_command_targets_job_id():
    from dispatch.slurm_adapter import build_scancel_command

    cmd = build_scancel_command("999")

    assert cmd == ["scancel", "999"]
