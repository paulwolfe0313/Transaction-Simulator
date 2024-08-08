import argparse
import csv
import random
from collections import defaultdict

def parse_arguments():
    parser = argparse.ArgumentParser(description="Transaction Simulator")
    parser.add_argument("cycles", type=int, help="Maximum number of cycles for the simulation")
    parser.add_argument("trans_size", type=int, help="Size of the transaction in number of operations")
    parser.add_argument("start_prob", type=float, help="Probability of a transaction starting in this cycle")
    parser.add_argument("write_prob", type=float, help="Probability that each operation is a write")
    parser.add_argument("rollback_prob", type=float, help="Probability of a rollback operation")
    parser.add_argument("timeout", type=int, help="Timeout wait")
    return parser.parse_args()

class Database:
    def __init__(self, filename="db.csv"):
        self.filename = filename
        self.data = self.load_database()

    def load_database(self):
        try:
            with open(self.filename, mode='r') as file:
                reader = csv.reader(file)
                data = next(reader)
                return [int(val) for val in data]
        except FileNotFoundError:
            return [0] * 32

    def save_database(self):
        with open(self.filename, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(self.data)

    def read(self, index):
        return self.data[index]

    def write(self, index, value):
        self.data[index] = value

class LockManager:
    def __init__(self):
        self.shared_locks = defaultdict(set)  # Track shared locks
        self.exclusive_locks = [None] * 32    # Track exclusive locks

    def acquire_shared_lock(self, tid, index):
        if self.exclusive_locks[index] is None:
            self.shared_locks[index].add(tid)
            return True
        return False

    def acquire_exclusive_lock(self, tid, index):
        if self.exclusive_locks[index] is None and not self.shared_locks[index]:
            self.exclusive_locks[index] = tid
            return True
        return False

    def release_locks(self, tid):
        for index in range(32):
            if self.exclusive_locks[index] == tid:
                self.exclusive_locks[index] = None
            if tid in self.shared_locks[index]:
                self.shared_locks[index].remove(tid)

class RecoveryManager:
    def __init__(self, filename="log.csv"):
        self.filename = filename
        self.log = self.load_log()

    def load_log(self):
        try:
            with open(self.filename, mode='r') as file:
                reader = csv.reader(file)
                return [tuple(row) for row in reader]
        except FileNotFoundError:
            return []

    def log_operation(self, tid, did, operation, value=None):
        if value is not None:
            self.log.append((str(tid), str(did), str(value), str(operation)))
        else:
            self.log.append((str(tid), str(did), str(operation)))

    def write_log(self):
        with open(self.filename, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(self.log)

    def apply_log(self, db):
        for entry in self.log:
            tid, did, *rest = entry
            operation = rest[-1]
            if operation == 'F':
                db.write(int(did), 1)
            elif operation == 'R':
                db.write(int(did), 0)

def clear_log_file(log_file="log.csv"):
    # Clear the log file
    open(log_file, 'w').close()

def clear_database_file(db_file="db.csv"):
    # Clear the database file by setting all values to 0
    with open(db_file, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([0] * 32)

def simulate_transactions(args):
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

                # Handle non-write operations or potential commit
                if len(active_transactions) >= args.trans_size:
                    recovery_manager.log_operation(tid, -1, 'C')
                    lock_manager.release_locks(tid)
                    active_transactions.remove(tid)

            # Randomly decide if the transaction should rollback
            if random.random() < args.rollback_prob:
                recovery_manager.log_operation(tid, -1, 'R')
                lock_manager.release_locks(tid)
                active_transactions.remove(tid)

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
