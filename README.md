## Octobots: A Multi-Threaded Python Data Pipeline

---
## Warning
<i>
I've developed this project to automate fetching data from multiple sources, such as Oracle DBs and MSSQL Server DBs. At the time, I had a very tight schedule, only a month or so to implement all the features I could think of.

That being said, this is a **very untidy codebase** that has a lot of **rooms for improvement**, e.g. no module and class has been written, no docstring inside classes and functions, etc.

Since I no longer have access to the infrastructure I used to develop, the iteration speed is much slower. See `TODOs` section below for current pending tasks.

</i>

---

Main entry point: `octobots.py`

### What it does

The program uses table address and database credentials input by user to locate the table, fetch metadata information, then create jobs to transfer data from source DB to target DB. It works in a multi-threaded fashion.

### Connection management

DB Connection credentials are stored as encrypted strings in `cxns`.

`aes_encryption.py` handles encryption and decryption.
Secrets such as PASSWORD and SALT are kept safe inside `_SECRETS.py` and are private to each user.

Template for `_SECRETS.py`:

```Python
PASSWORD = "some-secret-string"
SALT = b"another-secret-string"
```

`connections.py` makes use of `aes_encryption.py`, reads and writes to `cxns`. It also creates, tests and manages connections for the program.

### Job profiles

`Octobots` makes use of multiple threads to accelerate data pipeline. In order to do that, it has to create a pool of jobs in demand and execute these jobs. `jobProfiles.py` resolves that problem.

### Working with databases

- `libMSSQL.py` handles connection, metadata fetching and data writing from and to tables in Microsoft SQL Server Database.
- `libOracle.py` does the same jobs for Oracle Database.

- `typesHandler.py` handles conversion of data types between these databases.

### Utility modules

- `scheduler.py` handles scheduling so the program can run jobs at time users are not present at the computer.
- `getfootprint.py` calculates size of the data being transfered.
- `crashReport.py` tries to save information about the program when it crashes and why (if possible).
- `utils.py` works on utility functions such as version keeping, getting user input, printing info, time conversion, etc.
- `TableIt.py` is a 3rd party library that handles table printing for better command line interface.

### TODOs:

- Structure codebase into modules with `__init__`, and optionally `setup.py`
- Structure functions into classes
- Write docstrings for documentation
- More tasks TBD...
