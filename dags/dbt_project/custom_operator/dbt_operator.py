import yaml
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from dbt.cli.main import dbtRunner, dbtRunnerResult


class DbtCoreOperator(BaseOperator):

    template_fields = (
        "dbt_project_dir",
        "dbt_profiles_dir",
        "dbt_command",
        "target",
        "select",
        "dbt_vars",
    )

    def __init__(
        self,
        dbt_project_dir: str,
        dbt_profiles_dir: str,
        dbt_command: str,
        target: str = None,
        select: str = None,
        dbt_vars: dict = None,
        full_refresh: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.dbt_command = dbt_command
        self.dbt_project_dir = dbt_project_dir
        self.dbt_profiles_dir = dbt_profiles_dir
        self.target = target
        self.select = select
        self.dbt_vars = dbt_vars or {}
        self.full_refresh = full_refresh
        self.runner = dbtRunner()

    def execute(self, context):
        command_args = [
            self.dbt_command,
            "--profiles-dir",
            self.dbt_profiles_dir,
            "--project-dir",
            self.dbt_project_dir,
        ]

        if self.target:
            command_args.extend(["--target", self.target])

        if self.select:
            command_args.extend(["--select", self.select])

        if self.full_refresh:
            command_args.append("--full-refresh")

        if self.dbt_vars:
            vars_yaml = yaml.safe_dump(self.dbt_vars, default_flow_style=True).strip()
            command_args.extend(["--vars", vars_yaml])

        self.log.info("Running dbt command: %s", " ".join(command_args))

        try:
            res: dbtRunnerResult = self.runner.invoke(command_args)
        except Exception as exc:
            self.log.exception("dbt runner exception")
            raise AirflowException(f"dbt invoke failed: {exc}") from exc

        # --- no result ---
        if not res:
            raise AirflowException("dbt command returned no result object.")

        # --- raw logs ---
        if hasattr(res, "logs") and res.logs:
            self.log.info("dbt logs:")
            for log_line in res.logs:
                self.log.info(log_line.strip())

        # --- Check execute success ---
        #if hasattr(res, "success") and not res.success:
        #    raise AirflowException("dbt reported failure status.")

        if not getattr(res, "result", None):
            raise AirflowException("dbt executed but returned no run results (no models run).")

        # --- Summary results ---
        summary = {"dbt_command": self.dbt_command, "results": []}
        for r in res.result:
            node_name = getattr(r.node, "name", "unknown")
            status = getattr(r, "status", "unknown")
            execution_time = getattr(r, "execution_time", 0)
            message = getattr(r, "message", "")

            if status.lower() == "error":
                self.log.error(f"Model FAILED: {node_name} | Time: {execution_time:.2f}s | Error: {message}")
            else:
                self.log.info(f"Model: {node_name} | Status: {status} | Time: {execution_time:.2f}s | {message}")

            summary["results"].append(
                {
                    "node": node_name,
                    "status": status,
                    "time": execution_time,
                    "message": message,
                }
            )

            # raise exception if at least model failed
            if any(r["status"].lower() == "error" for r in summary["results"]):
                raise AirflowException("At least dbt models failed. Check logs above for details.")

        self.log.info("dbt completed with %d results", len(summary["results"]))
        return summary

