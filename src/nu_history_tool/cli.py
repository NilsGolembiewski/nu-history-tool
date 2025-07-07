import subprocess
import typer
import polars as pl
from datetime import datetime, date
from dateutil import tz
from rich.console import Console
from rich.table import Table
from typing import Optional
import re

app = typer.Typer()

def get_nu_history() -> pl.DataFrame:
    """Executes the nushell history command and returns a polars DataFrame."""
    # Execute the command in a login shell to load the full environment
    result = subprocess.run(
        ["nu", "-l", "-c", 'history | to csv'],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise typer.Exit(f"Failed to get nushell history: {result.stderr}")

    try:
        df = pl.read_csv(result.stdout.encode('utf-8'))
        if "start_timestamp" not in df.columns:
            typer.echo("Error: 'start_timestamp' column not found in Nushell history.")
            typer.echo(f"Available columns are: {df.columns}")
            typer.echo("This might happen if the script's environment doesn't load your full Nushell configuration.")
            raise typer.Exit(code=1)

        # Drop rows where start_timestamp is null
        df = df.filter(pl.col("start_timestamp").is_not_null())

        # Parse datetime with timezone using strptime
        df = df.with_columns(pl.col("start_timestamp").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S%.f %z"))
        return df
    except Exception as e:
        raise typer.Exit(f"Failed to parse Nushell history CSV: {e}")

def parse_date_or_datetime(value: str):
    """Parses a string into a date or datetime object. Supports 'YYYY-MM-DD', ISO datetime, or 'hh:mm' (assumed today)."""
    if not value:
        return None
    try:
        # If value matches hh:mm (optionally with seconds)
        time_match = re.fullmatch(r"(\d{1,2}):(\d{2})(?::(\d{2}))?", value)
        if time_match:
            hour = int(time_match.group(1))
            minute = int(time_match.group(2))
            second = int(time_match.group(3)) if time_match.group(3) else 0
            today = datetime.now(tz=tz.tzlocal()).date()
            dt = datetime.combine(today, datetime.min.time().replace(hour=hour, minute=minute, second=second))
            return dt.replace(tzinfo=tz.tzlocal())

        # If 'T' is not in the value, treat it as a date.
        if 'T' not in value:
            return datetime.strptime(value, "%Y-%m-%d").date()

        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            # If naive, assume local timezone.
            return dt.replace(tzinfo=tz.tzlocal())
        else:
            # If aware, convert to local timezone to normalize.
            return dt.astimezone(tz.tzlocal())
    except ValueError:
        raise typer.BadParameter(
            f"Invalid format for '{value}'. Use 'YYYY-MM-DD', ISO datetime (e.g., 'YYYY-MM-DDTHH:MM:SS'), or 'hh:mm'."
        )


@app.command()
def main(
    start: Optional[str] = typer.Option(
        None, "--start", "-s", help="Start date or datetime for filtering history (e.g., YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS). Assumed to be in your local timezone if no timezone is provided."
    ),
    end: Optional[str] = typer.Option(
        None, "--end", "-e", help="End date or datetime for filtering history (e.g., YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS). Assumed to be in your local timezone if no timezone is provided."
    ),
    output_format: str = typer.Option(
        "table",
        "--format",
        "-f",
        help="Output format (table, json, csv).",
        case_sensitive=False,
    ),
):
    """
    Nushell history viewer with date filtering and multiple output formats.
    """
    df = get_nu_history()

    if start:
        start_val = parse_date_or_datetime(start)
        if isinstance(start_val, date) and not isinstance(start_val, datetime):
            df = df.filter(pl.col("start_timestamp").dt.date() >= start_val)
        else:
            df = df.filter(pl.col("start_timestamp") >= start_val)

    if end:
        end_val = parse_date_or_datetime(end)
        if isinstance(end_val, date) and not isinstance(end_val, datetime):
            df = df.filter(pl.col("start_timestamp").dt.date() <= end_val)
        else:
            df = df.filter(pl.col("start_timestamp") <= end_val)

    if df.height == 0:
        typer.echo("No history found for the given date range.")
        raise typer.Exit()

    # Select and rename columns for output
    output_df = df.select([
        pl.col("command"),
        pl.col("cwd"),
        pl.col("start_timestamp").alias("start_time")
    ])

    if output_format == "table":
        console = Console()
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Command")
        table.add_column("CWD")
        table.add_column("Start Time")

        for row in output_df.iter_rows(named=True):
            start_time = row["start_time"]
            # Convert to local timezone if possible
            if isinstance(start_time, datetime):
                if start_time.tzinfo is not None:
                    start_time_local = start_time.astimezone(tz.tzlocal())
                else:
                    start_time_local = start_time
                start_time_str = str(start_time_local)
            else:
                start_time_str = str(start_time)
            table.add_row(row["command"], row["cwd"], start_time_str)

        console.print(table)
    elif output_format == "json":
        print(output_df.write_json(row_oriented=True))
    elif output_format == "csv":
        print(output_df.write_csv())
    else:
        typer.echo(f"Unknown output format: {output_format}")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
