import threading
import socket
import json
import time

class Process():
    def __init__(self, process_id, all_process_ids, current_term, prev_log_index, prev_log_term, leader_commit, current_entry):
        super().__init__()
        self.id = process_id
        self.all_process_ids = all_process_ids  # 全プロセスIDのリスト
        self.is_leader = False
        
        self.current_term = 1
        self.leader_id = 10001  # リーダー固定
        self.prev_log_index = prev_log_index
        self.prev_log_term = prev_log_term
        self.entries = []
        self.leader_commit = leader_commit


    def input_logs(self):
        #別スレッドで入力を受け取る
        while True:
            entry = input("enter command to add")
            self.entries.append(entry)
            print(str(entry) + "をリーダーのログに追加しました")
            print(str(self.entries) + "現在のログ")


    def append_entries(self):
        message_append_entries = {
        "type" : "APPEND_ENTRIES",
        #"id" : "self.id",
        "term" : self.current_term,
        "leaderID" : self.leader_id,
        "prevLogIndex" : self.prev_log_index,
        "prevLogTerm" : self.prev_log_term,
        "entries" : self.entries,
        "leaderCommit" : self.leader_commit
        }

        for id in self.all_process_ids:
            self.send_message(id, message_append_entries)
        print("AppendEntriesRPC")
        time.sleep(1.0)




    def keep_listening(self):
        #ソケット通信でデータを受信する
        #データを受信したら別スレッドでhandle_messageを呼び出す
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(("127.0.0.1", self.id))
        sock.listen()

        while True:
            client_sock, sender_addr = sock.accept()
            data = client_sock.recv(1024)
            message = json.loads(data.decode("UTF-8"))
            t = threading.Thread(target=self.handle_message, args=(message,))
            t.start()

        pass

    def send_message(self, target_port, message):
        #ソケット通信でデータを送信する
        #メッセージにメッセージタイプを付与することで受信側が handle_message() で識別できるようにする

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
        try: 
            sock.connect(("127.0.0.1", target_port))
            json_message = json.dumps(message).encode("UTF-8")
            sock.send(json_message)
        except ConnectionRefusedError:
        #     print(f"[Process {self.id}] Process {target_port} is not available.")
            pass
        finally:
            sock.close()


    def handle_message(self, message):
        #keep_listeningから呼ばれ、受信したメッセージを処理する
        #メッセージタイプに応じて別の処理を行う
        message_type = message.get("type")

        if message_type == "APPEND_ENTRIES":
            message_term = message.get("term")
            message_leaderid = message.get("leaderID")
            message_previndex = message.get("prevLogIndex")
            message_prevterm = message.get("prevLogTerm")
            message_entries = message.get("entries")
            message_leadercommit = message.get("leaderCommit")
            print("ターム" + str(message_term))
            print("リーダー"+ str(message_leaderid)+str(message_entries))      

    def run(self):
        print(f"[Process {self.id}] 起動しました。")
        #個別スレッドとしてソケット通信を待つkeep_listeningを起動
        listener_thread = threading.Thread(target=self.keep_listening)
        listener_thread.daemon = True
        listener_thread.start()

        while True:
            if self.id == 10001:
                self.is_leader = True

            if self.is_leader == True:
                self.append_entries()
                print("自分がリーダー")               
                time.sleep(2.0)


if __name__ == "__main__":
    # 簡単なテストのためにプロセスIDをいくつか定義
    process_ids = [10001, 10002, 10003]

    # ユーザーにどのプロセスを起動するか選ばせる
    #0なら10001、1なら10002、2なら10003を起動という具合
    index = input("Enter index (0, 1, or 2) to kill corresponding process after start: ")
    index = int(index) if index.isdigit() else None
    p = Process(
        process_ids[index],
        process_ids,
        1,
        0,
        0,
        0
        ) 
    
    #コマンド入力用のスレッド
    input_thread = threading.Thread(target=p.input_logs)
    input_thread.daemon = True
    input_thread.start()
    p.run()