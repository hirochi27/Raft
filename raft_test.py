import threading
import time

from raft import Process

def auto_input_logs(process, count=10000):
    for i in range(count):
        entry = f"entry_{i}"
        log_entry = {
            "term":process.current_term,
            "entry": f"entry_{i}"
        }
        process.log.append(log_entry)
        process.match_index[process.leader_id] = len(process.log) - 1
        if i % 100 == 0:
              print(f"[{process.id}] {i}/{count} 件追加")
        time.sleep(0.1)

def save_log_to_file(process):
    if process.id == 10001:
        filename = "leader.txt"
    else:
        filename = f"{process.id}.txt"
    with open(filename, "w") as f:
        f.write(str(process.state_machine))

if __name__ == "__main__":
      process_ids = [10001, 10002, 10003]
      index = int(input("0, 1, or 2: "))

      p = Process(process_ids[index], process_ids, 1, 0, 0, 0, 0)

      if p.id == 10001:
          t = threading.Thread(target=auto_input_logs, args=(p, 10000))
          t.daemon = True
          t.start()

      try:
            p.run()
      except KeyboardInterrupt:
            save_log_to_file(p)