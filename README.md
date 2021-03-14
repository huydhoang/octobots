## Octobots: A Multi-Threaded Python Data Pipeline

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

### TODO:

- Find relevant use cases for a Python data pipeline
- Architecture for a more modern data streaming pipeline
- Rewriting modules in standard Python classes
- More tasks TBD...
