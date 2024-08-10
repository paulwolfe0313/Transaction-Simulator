# Transaction Simulator

## Overview

This Python program simulates a simple transaction management system that handles concurrent transactions with support for both exclusive and shared locks. The simulator logs all transactions and operations and can recover the database to its most recent state in the event of a crash.

## Features

- **Transaction Management**: Simulates concurrent transactions with start, commit, and rollback operations.
- **Concurrency Control**: Implements shared and exclusive locks to manage read and write operations on database items.
- **Logging and Recovery**: Logs all operations performed by transactions. On startup, applies the log to restore the database to its most recent state.

## How to Download and Run the Code

1. **Clone the Repository**:
   - Open your preferred IDE (e.g., VS Code).
   - Open a terminal or use Git Bash.
   - Clone the repository using the following command:
     ```bash
     git clone https://github.com/paulwolfe0313/Transaction-Simulator.git
     ```

2. **Open the Project**:
   - In VS Code, go to `File` > `Open Folder` and select the `Transaction-Simulator` folder.
   - Open a new terminal within VS Code by navigating to `Terminal` > `New Terminal`.

3. **Run the Program**:
   - Navigate to the directory containing `transaction_simulator.py` using the terminal.
   - Run the program with an example command:
     ```bash
     python transaction_simulator.py 20 5 0.5 0.4 0.1 10
     ```

## Command-Line Arguments

- **cycles**: Maximum number of cycles for the simulation.
- **trans_size**: Maximum number of operations in a single transaction.
- **start_prob**: Probability that a transaction will start in any given cycle.
- **write_prob**: Probability that any operation in a transaction will be a write operation.
- **rollback_prob**: Probability that a transaction will rollback instead of committing.
- **timeout**: Number of cycles to wait if a transaction is blocked.

## Log and Database Files

- The log of all transactions is stored in `log.csv`.
- The state of the database is stored in `db.csv`.

## What the Simulation Does

### Transaction Lifecycle:

- **Determines** if a transaction should start based on a given probability.
- **Simulates** operations (read/write) within each transaction, considering the available locks.
- **Handles blocking** of transactions if they cannot acquire the necessary lock.
- **Logs** each operation performed by transactions, including start, read, write, commit, and rollback.

### Concurrency Control:

- **Implements** shared locks for read operations and exclusive locks for write operations.
- **Manages** lock contention using a timeout mechanism, where blocked transactions wait for a specified number of cycles before reattempting.

### Recovery:

- **On startup**, the simulation applies the log to restore the database to its most recent state.
- **After every 25 cycles**, the log is written to disk to ensure the database can be recovered.

## Notes

- The simulation clears both the log and database files before each run, ensuring that each simulation starts with a clean state.
- The database consists of 32-bit data, with each bit initially set to 0.
- Commit operations are logged only if the number of active transactions reaches the `trans_size` limit, ensuring that transactions are fully committed only under defined conditions.
