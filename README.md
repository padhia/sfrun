# sfrun

Run a Snwoflake SQL query or a Snowpark Python function. Produces output in either text, csv, markdown or Excel formats

## sfrunb

*sfrunb* is designed to run a batch of Snowflake SQLs. Unlike *sfrun*:

- it can run multiple scripts
- scripts can contain more than one SQL statement
- SQL statement can be of any type, including DML and DCL
- only supports text format for output
