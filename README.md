# sfrun

Run a Snowflake SQL query or a Snowpark Python function from the command line and print the result in a variety of formats: text table, CSV, TSV, markdown, JSON, JSON Lines or MS Excel.

The package installs two commands:

- `sfrun`: run one or more queries (or Snowpark scripts) and format each result set
- `sfrunb`: run a batch of SQL scripts containing any type of statement (see [sfrunb](#sfrunb))

## Installation

Requires Python 3.12 or later.

```sh
pip install .            # from a clone of this repository
nix run . -- --help      # or, using the Nix flake
```

## Connecting to Snowflake

Both commands use connections defined in the Snowflake `connections.toml` file.

| Option              | Description                             |
| ------------------- | --------------------------------------- |
| `-c`, `--conn NAME` | connection name from `connections.toml` |
| `--database NAME`   | override or set the default database    |
| `--schema NAME`     | override or set the default schema      |
| `--role NAME`       | override or set the default role        |
| `--warehouse NAME`  | override or set the default warehouse   |

## SQL templates

Both commands render every SQL script read from a file as a [Jinja2](https://jinja.palletsprojects.com/) template before running it. SQL given with `-q` or read from standard input is used as-is.

Environment variables are available as `ctx.env.NAME`:

```sql
-- orders.sql
select *
from {{ ctx.env.DB }}.sales.orders
{% if ctx.env.REGION is defined %}where region = '{{ ctx.env.REGION }}'{% endif %}
;
```

```sh
DB=prod REGION=EMEA sfrun -c dev orders.sql
```

- Referencing an unset variable is an error (`environment variable 'DB' is not set`). Use `{% if ctx.env.X is defined %}`, `ctx.env.get("X", "default")` or `{{ ctx.env.X | default("...") }}` for optional variables.
- `{% include "other.sql" %}` and other template references are resolved relative to the script's directory.
- SQL text containing `{{`, `{%` or `{#` is treated as template syntax; wrap such text in `{% raw %}...{% endraw %}`.

## sfrun

```
sfrun [options] [SCRIPT ...]
```

### Input

A query can come from any of the following sources. More than one may be given; each produces its own result.

| Option                        | Description                                                 |
| ----------------------------- | ----------------------------------------------------------- |
| `SCRIPT`, `-f`, `--file FILE` | SQL script (`.sql`, etc.) or Snowpark Python script (`.py`) |
| `-t`, `--table NAME`          | table or view name; runs `select * from NAME`               |
| `-q`, `--query SQL`           | SQL query text                                              |
| _(none)_                      | SQL is read from standard input                             |

`-f`, `-t` and `-q` may be repeated.

### Output format

| Option                | Description                                       |
| --------------------- | ------------------------------------------------- |
| `--format FORMAT`     | one of the formats listed below                   |
| `-o`, `--output FILE` | write output to `FILE` instead of standard output |

| Format  | Output                         | Guessed from extension |
| ------- | ------------------------------ | ---------------------- |
| `fmt`   | tabular text (default)         | `.fmt`                 |
| `csv`   | comma separated values         | `.csv`                 |
| `tsv`   | tab separated values           | `.tsv`                 |
| `raw`   | tab delimited, no headers      | `.raw`                 |
| `md`    | markdown table                 | `.md`                  |
| `json`  | JSON array of objects          | `.json`                |
| `jsonl` | JSON Lines, one object per row | `.jsonl`               |
| `xls`   | MS Excel workbook              | `.xlsx`, `.xls`        |

When `--format` is omitted, the format is guessed from the extension of the `--output` file (case-insensitive). Files with an unrecognized or no extension, and output to standard output, use `fmt`. An explicit `--format` always takes precedence over the file extension.

`--format=xls` requires `--output`, since Excel output cannot be written to standard output.

> **Note:** when several queries are given together with `--output`, each result overwrites the file, so only the last result is kept.

### Other options

| Option                                  | Description                                                                    |
| --------------------------------------- | ------------------------------------------------------------------------------ |
| `-l`, `--limit NUM`                     | fetch at most `NUM` rows; `0` for no limit                                     |
| `-P`, `--pretty-headers`                | turn column names into readable titles, e.g. `ORDER_DATE` becomes `Order Date` |
| `--show-sql` | print the SQL instead of running it; for Snowpark scripts, the SQL generated from the DataFrame |
| `--describe`, `--desc`, `--show-schema` | print the result columns (name, type, nullability, etc.) instead of the data; always printed as text |
| `--fn NAME`                             | name of the function to call in Snowpark scripts (default: `main`)             |
| `--version`                             | print the version and exit                                                     |

> **Note:** without `--limit`, output is cut off after **100 rows** with a warning, even when writing to a file. Use `-l 0` to export the full result.

### Snowpark scripts

A `.py` script must define a function (named `main` unless `--fn` is used) that takes a Snowpark `Session` and returns either a `DataFrame` or a SQL string:

```python
from snowflake.snowpark import DataFrame, Session
from snowflake.snowpark.functions import col


def main(session: Session) -> DataFrame:
    return session.table("orders").filter(col("status") == "OPEN")
```

### Examples

```sh
# print a table as text
sfrun -c dev -t sales.public.orders

# run a query, save the full result as CSV (format guessed from the extension)
sfrun -c dev -l 0 -q "select * from orders where order_date >= '2026-01-01'" -o orders.csv

# Excel output
sfrun -c dev -l 0 -t orders -o orders.xlsx

# write markdown to a file with a .txt extension
sfrun -c dev --format=md -t orders -o orders.txt

# SQL from standard input, JSON Lines to standard output
echo "select current_user() as usr" | sfrun -c dev --format=jsonl

# column metadata of a query
sfrun -c dev --describe -t orders

# Snowpark script
sfrun -c dev report.py --fn open_orders
```

## sfrunb

```
sfrunb [options] [FILE ...]
```

`sfrunb` runs batches of Snowflake SQL. Unlike `sfrun`:

- scripts can contain more than one SQL statement
- statements can be of any type, including DDL, DML and DCL
- output is text only

Each statement is printed followed by its result.

| Option                      | Description                                                                                     |
| --------------------------- | ----------------------------------------------------------------------------------------------- |
| `FILE`, `-f`, `--file FILE` | SQL script; `-` reads from standard input                                                       |
| `-t`, `--table NAME`        | table or view name; runs `select * from NAME`                                                   |
| `-q`, `--query SQL`         | SQL statement text                                                                              |
| `-p`, `--pretty`            | turn column names into readable titles                                                          |
| `-l`, `--limit NUM`         | maximum rows printed per result; `0` for no limit (default 500)                                 |
| `--on-error ACTION`         | `stop` (default), `continue` with the next statement, or `skip-file` to move to the next script |
| `-o`, `--out-dir DIR` | write each script file's output to `DIR/<script name>.log` instead of standard output |

`sfrunb` exits with status 1 if any statement failed.

```sh
sfrunb -c dev --on-error=skip-file setup.sql load.sql -o logs/
```
