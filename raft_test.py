import threading
import time
import random

from raft import Process


class ClientTest:
    def __init__(self, leader, processes):
        self.leader = leader
        self.processes = processes

        # 各プロセスごとのログファイルを初期化
        for p in processes:
            with open(f"{p.id}.txt", "w") as f:
                f.write(f"=== Process {p.id} state machine log ===\n")

    def create_random_command(self):
        op = random.choice(["SET", "SET", "SET", "DELETE"])
        key = random.choice(["a", "b", "c", "d", "e"])
        if op == "SET":
            value = str(random.randint(1, 100))
            return (op, key, value)
        else:
            return (op, key, None)

    def wait_commit(self, log_index):
        while self.leader.leader_commit < log_index:
            time.sleep(0.001)

    def dump_state_machines(self, step):
        for p in self.processes:
            line = f"[{step}] {p.state_machine}\n"
            print(f"[{p.id}] {line.strip()}")
            with open(f"{p.id}.txt", "a") as f:
                f.write(line)

    def run(self, num_requests=100):
        print(f"=== クライアントテスト開始 ({num_requests} requests) ===")

        for i in range(num_requests):
            command = self.create_random_command()

            # クライアント → リーダー
            log_entry = {
                "term": self.leader.current_term,
                "entry": command
            }
            self.leader.log.append(log_entry)

            log_index = len(self.leader.log) - 1
            self.leader.match_index[self.leader.leader_id] = log_index

            # commit を待つ
            self.wait_commit(log_index)

            self.dump_state_machines(i + 1)

            # 100回に1回 state_machine を出力・保存
            # if (i + 1) % 100 == 0:
            #     self.dump_state_machines(i + 1)

        # 最終状態
        time.sleep(0.1)
        self.dump_state_machines("FINAL")

        print("=== テスト終了 ===")


if __name__ == "__main__":
    process_ids = [10001, 10002, 10003]

    leader = Process(10001, process_ids, 1, 0, 0, 0, 0)
    follower1 = Process(10002, process_ids, 1, 0, 0, 0, 0)
    follower2 = Process(10003, process_ids, 1, 0, 0, 0, 0)

    processes = [leader, follower1, follower2]

    # フォロワー起動
    threading.Thread(target=follower1.run, daemon=True).start()
    threading.Thread(target=follower2.run, daemon=True).start()

    time.sleep(0.1)

    # テスト開始
    tester = ClientTest(leader, processes)
    test_thread = threading.Thread(target=tester.run, args=(100,), daemon=True)
    test_thread.start()

    # リーダー実行
    leader.run()
