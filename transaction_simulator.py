import argparse
import csv
import random

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
    def __init__(self):
        self.data = [0] * 32

    def read(self, index):
        return self.data[index]

    def write(self, index, value):
        self.data[index] = value
class LockManager:
    def __init__(self):
        self.locks = [None] * 32  # None means no lock, otherwise it holds the transaction ID

    def acquire_lock(self, tid, index):
        if self.locks[index] is None:
            self.locks[index] = tid
            return True
        return False

    def release_locks(self, tid):
        for i in range(32):
            if self.locks[i] == tid:
                self.locks[i] = None



class RecoveryManager:
    def __init__(self):
        self.log = []

    def log_operation(self, tid, did, operation):
        self.log.append((tid, did, operation))

    def write_log(self, filename="log.csv"):
        with open(filename, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(self.log)

    def apply_log(self, db):
        for tid, did, operation in self.log:
            if operation == 'write':
                db.write(did, 1)
            elif operation == 'rollback':
                db.write(did, 0)
            # Add more operations as needed



def simulate_transactions(args):
    db = Database()
    lock_manager = LockManager()
    recovery_manager = RecoveryManager()

    active_transactions = []
    cycle = 0

    while cycle < args.cycles:
        if random.random() < args.start_prob:
            tid = len(active_transactions) + 1
            active_transactions.append(tid)
            recovery_manager.log_operation(tid, -1, 'start')

        for tid in active_transactions:
            if random.random() < args.write_prob:
                did = random.randint(0, 31)
                if lock_manager.acquire_lock(tid, did):
                    db.write(did, 1)
                    recovery_manager.log_operation(tid, did, 'write')
                else:
                    if random.random() < args.rollback_prob:
                        recovery_manager.log_operation(tid, -1, 'rollback')
                        lock_manager.release_locks(tid)
                        active_transactions.remove(tid)

            if len(active_transactions) >= args.trans_size:
                recovery_manager.log_operation(tid, -1, 'commit')
                lock_manager.release_locks(tid)
                active_transactions.remove(tid)

        if cycle % 25 == 0:
            recovery_manager.write_log()

        cycle += 1

    recovery_manager.write_log()
if __name__ == "__main__":
    args = parse_arguments()
    simulate_transactions(args)
