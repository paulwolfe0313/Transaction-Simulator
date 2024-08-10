import argparse
import csv
import random
from collections import defaultdict

# Function to parse command-line arguments
def parse_arguments():
    """
    Parses the command-line arguments for the transaction simulator.
    Arguments:
    - cycles: The maximum number of cycles for the simulation.
    - trans_size: The maximum number of operations per transaction.
    - start_prob: The probability of a transaction starting in a given cycle.
    - write_prob: The probability that each operation is a write.
    - rollback_prob: The probability of a rollback operation.
    - timeout: The timeout for blocked transactions.
    
    Returns:
    - A namespace containing all the parsed arguments.
    """
    parser = argparse.ArgumentParser(description="Transaction Simulator")
    parser.add_argument("cycles", type=int, help="Maximum number of cycles for the simulation")
    parser.add_argument("trans_size", type=int, help="Size of the transaction in number of operations")
    parser.add_argument("start_prob", type=float, help="Probability of a transaction starting in this cycle")
    parser.add_argument("write_prob", type=float, help="Probability that each operation is a write")
    parser.add_argument("rollback_prob", type=float, help="Probability of a rollback operation")
    parser.add_argument("timeout", type=int, help="Timeout wait")
    return parser.parse_args()

# Class representing the database
class Database:
    """
    Simulates a simple database with 32-bit values. The database can read and write values,
    and it maintains its state in a CSV file.
    """
    def __init__(self, filename="db.csv"):
        self.filename = filename
        self.data = self.load_database()

    def load_database(self):
        """
        Loads the database state from a CSV file. If the file does not exist, it initializes 
        the database with 32 zeroes.
        """
        try:
            with open(self.filename, mode='r') as file:
                reader = csv.reader(file)
                data = next(reader)
                return [int(val) for val in data]
        except FileNotFoundError:
            return [0] * 32

    def save_database(self):
        """
        Saves the current state of the database to the CSV file.
        """
        with open(self.filename, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(self.data)

    def read(self, index):
        """
        Reads the value at the specified index in the database.
        """
        return self.data[index]

    def write(self, index, value):
        """
        Writes the specified value to the specified index in the database.
        """
        self.data[index] = value

# Class representing the lock manager
class LockManager:
    """
    Manages both shared and exclusive locks for database access. Each bit in the database
    can have either no lock, a shared lock by multiple transactions, or an exclusive lock
    by a single transaction.
    """
    def __init__(self):
        self.shared_locks = defaultdict(set)  # Track shared locks
        self.exclusive_locks = [None] * 32    # Track exclusive locks

    def acquire_shared_lock(self, tid, index):
        """
        Attempts to acquire a shared lock for the given transaction ID (tid) at the specified index.
        Shared locks can be acquired if there is no exclusive lock at the index.
        Returns True if the lock is successfully acquired, False otherwise.
        """
        if self.exclusive_locks[index] is None:
            self.shared_locks[index].add(tid)
            return True
        return False

    def acquire_exclusive_lock(self, tid, index):
        """
        Attempts to acquire an exclusive lock for the given transaction ID (tid) at the specified index.
        Exclusive locks can be acquired if there are no shared locks and no other exclusive lock.
        Returns True if the lock is successfully acquired, False otherwise.
        """
        if self.exclusive_locks[index] is None and not self.shared_locks[index]:
            self.exclusive_locks[index] = tid
            return True
        return False

    def release_locks(self, tid):
        """
        Releases all locks held by the given transaction ID (tid). This includes both shared and exclusive locks.
        """
        for index in range(32):
            if self.exclusive_locks[index] == tid:
                self.exclusive_locks[index] = None
            if tid in self.shared_locks[index]:
                self.shared_locks[index].remove(tid)

# Class representing the recovery manager
class RecoveryManager:
    """
    Manages the logging and recovery process. Logs all transactions and operations,
    and can apply these logs to recover the database state.
    """
    def __init__(self, filename="log.csv"):
        self.filename = filename
        self.log = self.load_log()

    def load_log(self):
        """
        Loads the transaction log from a CSV file. If the file does not exist, it initializes an empty log.
        """
        try:
            with open(self.filename, mode='r') as file:
                reader = csv.reader(file)
                return [tuple(row) for row in reader]
        except FileNotFoundError:
            return []

    def log_operation(self, tid, did, operation, value=None):
        """
        Logs an operation (e.g., start, write, commit, rollback) for a transaction.
        If the operation involves reading a value, that value is also logged.
        """
        if value is not None:
            self.log.append((str(tid), str(did), str(value), str(operation)))
        else:
            self.log.append((str(tid), str(did), str(operation)))

    def write_log(self):
        """
        Writes the current log to the CSV file.
        """
        with open(self.filename, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(self.log)

    def apply_log(self, db):
        """
        Applies the operations recorded in the log to the database. This is used for recovering the
        database state by reapplying logged operations.
        """
        for entry in self.log:
            tid, did, *rest = entry
            operation = rest[-1]
            if operation == 'F':
                db.write(int(did), 1)
            elif operation == 'R':
                db.write(int(did), 0)

# Function to clear the log file
def clear_log_file(log_file="log.csv"):
    """
    Clears the contents of the log file before starting a new simulation.
    """
    open(log_file, 'w').close()

# Function to clear the database file
def clear_database_file(db_file="db.csv"):
    """
    Clears the database file by setting all values to 0. This resets the database state before
    starting a new simulation.
    """
    with open(db_file, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([0] * 32)

# Main function to simulate transactions
def simulate_transactions(args):
    """
    Simulates a series of transactions based on the provided command-line arguments.
    This includes transaction starts, read/write operations, commits, rollbacks, and
    managing locks and logging.
    """
    db = Database()
    lock_manager = LockManager()
    recovery_manager = RecoveryManager()

    # Apply the log to the database at the start
    recovery_manager.apply_log(db)

    active_transactions = []
    blocked_transactions = {}
    cycle = 0

    while cycle < args.cycles:
        # Determine if a new transaction starts
        if random.random() < args.start_prob:
            tid = len(active_transactions) + 1
            active_transactions.append(tid)
            recovery_manager.log_operation(tid, -1, 'S')

        for tid in active_transactions[:]:
            # Check if transaction is blocked
            if tid in blocked_transactions:
                if blocked_transactions[tid] > 0:
                    blocked_transactions[tid] -= 1
                    continue
                else:
                    del blocked_transactions[tid]

            did = random.randint(0, 31)
            transaction_complete = False  # To track if a transaction has been completed (commit/rollback)

            # Randomly decide if the operation is a write or not
            if random.random() < args.write_prob:
                if lock_manager.acquire_exclusive_lock(tid, did):
                    db.write(did, 1)
                    recovery_manager.log_operation(tid, did, 'F')
                else:
                    # Block transaction if lock cannot be acquired
                    blocked_transactions[tid] = args.timeout
            else:
                if lock_manager.acquire_shared_lock(tid, did):
                    value = db.read(did)
                    recovery_manager.log_operation(tid, did, 'F', value)
                else:
                    # Block transaction if shared lock cannot be acquired
                    blocked_transactions[tid] = args.timeout

            # Randomly decide if the transaction should rollback
            if random.random() < args.rollback_prob:
                recovery_manager.log_operation(tid, -1, 'R')
                lock_manager.release_locks(tid)
                active_transactions.remove(tid)
                transaction_complete = True

            # Handle commit if transaction size limit is reached
            if len(active_transactions) >= args.trans_size and not transaction_complete:
                recovery_manager.log_operation(tid, -1, 'C')
                lock_manager.release_locks(tid)
                active_transactions.remove(tid)
                transaction_complete = True

        if cycle % 25 == 0:
            recovery_manager.write_log()

        cycle += 1

    # Final log write and apply log to database
    recovery_manager.write_log()
    db.save_database()

if __name__ == "__main__":
    args = parse_arguments()
    clear_log_file()  # Clear log file before starting the simulation
    clear_database_file()  # Clear database file before starting the simulation
    simulate_transactions(args)
