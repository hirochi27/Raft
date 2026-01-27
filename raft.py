import threading
import socket
import json
import time

class logger:
    def __init__(self, process_id):
        self.process_id = process_id
        self.filename = f"log_{process_id}_2.txt"
        self.state_machine_filename = f"state_machine_{process_id}.txt"
    
        with open(self.filename, "w") as file:
            file.write(f"=== Process {self.process_id} state machine log ===\n")
    
        with open(self.state_machine_filename, "w") as file:
            file.write(f"=== Process {self.process_id} state machine log ===\n")


    def set(self, message):
        with open(self.filename, "a") as file:
            file.write(str(message) + "\n")
        print(message)
        #initで作ったファイルに書き込み

    def set_state_machine(self, state_machine, process_id):
        message = f"[{process_id}] ステートマシン　=　{state_machine}"
        with open(self.state_machine_filename, "a") as file:                         
            file.write(message + "\n")

        

class Process():
    def __init__(self, process_id, all_process_ids, prev_log_index, prev_log_term, leader_commit, next_index, append_entries_success ):
        super().__init__()
        self.connections = {}
        self.conn_lock = threading.Lock()

        self.id = process_id
        self.all_process_ids = all_process_ids  # 全プロセスIDのリスト
        self.is_leader = False

        self.current_term = 1
        self.leader_id = 10001  # リーダー固定
        self.prev_log_index = prev_log_index
        self.prev_log_term = prev_log_term
        self.entries = [] #フォロワーに送るエントリ
        self.leader_commit = -1

        self.log = [] #持ってるログ（ターム、エントリ）
        self.next_index = {
            10001: 0,#0を最新のリーダーのエントリの次にしたい
            10002: 0,
            10003: 0
        }

        self.append_entries_success = False
        #for pid in all_process_ids:
          #self.next_index[pid] = 0
        self.sent_entries_len = {
            10001: 0,
            10002: 0,
            10003: 0
        }

        self.last_applied = -1

        self.state_machine = {}

        self.match_index = {
            10001: -1,
            10002: -1,
            10003: -1
        }

        self.logger = logger(self.id)

    def input_logs(self):
        #別スレッドで入力を受け取る
        #自身のログに追加
        #currentIndexとprevIndex,Termを抽出
        while True:
            line = input("enter command (例： SET x 1)")
            parts = line.split()
            if len(parts) >= 3 and parts[0].upper() == "SET":
                entry = (parts[0].upper(), parts[1], parts[2])
            elif len(parts) >= 2 and parts[0].upper() == "DELETE":
                entry = (parts[0].upper(), parts[1], None)
            elif len(parts) >= 2 and parts[0].upper() == "GET":
                entry = (parts[0].upper(), parts[1], None)
            else:
                self.logger.set("無効なコマンド")


            log_entry = {
                "term" : self.current_term,
                "entry" : entry
            }

            self.log.append(log_entry)#リーダーのログに追加
            self.logger.set(f"[{self.id}] ログ追加: {entry}")
            self.logger.set(f"現在のログ {self.log}")

            self.match_index[self.leader_id] = len(self.log) -1 #リーダーのmatchindex?
            self.logger.set(f"[{self.id}] matchIndex[{self.leader_id}]={self.match_index[self.leader_id]} (リーダー自身)")
            time.sleep(2)
            
    def output_entries(self):
        pass

    def append_entries(self):

        for id in self.all_process_ids:

            if id == self.leader_id:
                continue

            self.entries = self.log[self.next_index[id]:]#送るエントリは、logのnextindexから先
            self.sent_entries_len[id] = len(self.entries)
            self.prev_log_index = self.next_index[id]-1 #prevlogindexはnextの一個前
            if self.prev_log_index == -1:#prevlogindexが-1のときprevlogrermは１
                self.prev_log_term = 1 #ターム数の変更実装前なのでとりあえず１
            elif 0 <= self.prev_log_index < len(self.log):
                self.prev_log_term = self.log[self.prev_log_index]["term"]
            else:
                self.logger.set(f"Warning: prev_log_index {self.prev_log_index} is out of range for log length {len(self.log)}")
                self.prev_log_term = 1
                #logの中の、インデックス＝prevlogindexに含まれるキーtermの値を取得

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
            #フォロワーから返信受け取ってから以下のプリント
            #self.logger.set("prevlogインデックス"+str(self.prev_log_index)+"prevlogターム"+ str(self.prev_log_term))

            self.send_message(id, message_append_entries)
            self.logger.set(f"[{self.id}→{id}] AppendEntriesRPC送信 entries = {self.entries}")
            time.sleep(2)

    def on_append_entries(self, term,  leader_id, prev_log_index, prev_log_term, entries, leader_commit):
        #appendEintriesを受け取ったフォロワー側の処理
        #prevlogindex,prevlogtermを受け取ってOkの時はsuccessをTrue。一致しない時はFalse
        #これをリーダーへ返す
        self.logger.set(f"[{self.id}] ← {leader_id} AppendEntries受信 entries={entries}")
        self.leader_commit = leader_commit
        self.apply_to_state_machine()
        self.logger.set(f"[{self.id}] leader commit受信={leader_commit}")
        self.logger.set(f"[{self.id}] ステートマシン適用 commit={leader_commit}")

        if term < self.current_term:  
            self.append_entries_success = False
            return
        self.current_term = term
        if prev_log_index == -1:
            for e in entries:
                self.log.append(e)
            self.append_entries_success = True
            self.logger.set(f"[{self.id}] ログ追加完了 {self.log}")
            self.logger.set(f"[{self.id}] → {self.leader_id} 応答: True")
        elif 0 <= prev_log_index < len(self.log):
            if prev_log_term == self.log[prev_log_index]["term"]:
                self.log = self.log[:prev_log_index + 1]
                for e in entries:
                    self.log.append(e)
                self.append_entries_success = True
                self.logger.set(f"[{self.id}] ログ追加完了 {self.log}")
                self.logger.set(f"[{self.id}] → {self.leader_id} 応答: True")
            else:
                self.append_entries_success = False
                self.logger.set(f"[{self.id}] → {self.leader_id} 応答: False")
        else:
            self.append_entries_success = False
            self.logger.set(f"[{self.id}] → {self.leader_id} 応答: False")


        response = {
            "type" : "RESPONSE",
            "from_id" : self.id,
            "success" :  self.append_entries_success
            }

        #self.logger.set(f"[{self.id}] log={self.log}")

        self.send_message(self.leader_id, response)
        #self.logger.set("!!self.append_entries_success!!")

    def on_append_entries_response(self, from_id, success):
        #true,Falseを受け取ったら
        #trueの場合、nextindexを"送ったエントリの次"にする
        #self.logger.set("success")
        if success == True:
            #self.logger.set("True")
            count = self.sent_entries_len.get(from_id, 0)
            self.next_index[from_id] = self.next_index[from_id] + count#送ったエントリ数文next_indexを増やす
            self.logger.set(f"[{self.id}] ← {from_id} 応答受信: {success}")
            self.logger.set(f"[{self.id}] nextIndex[{from_id}]={self.next_index[from_id]}")
            self.match_index[from_id] = self.next_index[from_id] - 1 #コミットのためのどこまで複製したかの追跡
            self.logger.set(f"[{self.id}] matchIndex[{from_id}]={self.match_index[from_id]}")
        elif success == False:
            #self.logger.set("False")
            self.next_index[from_id] = max(0, self.next_index[from_id] -1)
            self.logger.set(f"[{self.id}] ← {from_id} 応答受信: failure")
            self.logger.set(f"nextIndex={self.next_index[from_id]}")

        all_match_index_value = list(self.match_index.values())
        all_match_index_value.sort()#昇順？に並べる→昇順にしたら中心にいるのはぜったい過半数
        majority_index = len(all_match_index_value) // 2 #len(self.all_process_ids) / 2 + 1
        old_commit = self.leader_commit
        self.leader_commit = all_match_index_value[majority_index]
        self.logger.set(f"[{self.id}] matchIndex={self.match_index} 過半数={self.leader_commit}")
        if old_commit != self.leader_commit:
            self.logger.set(f"[{self.id}] leader_commit更新: {old_commit} → {self.leader_commit}")
        self.apply_to_state_machine()
        self.logger.set(f"[{self.id}] ステートマシン適用 commit={self.leader_commit}")

    def apply_to_state_machine(self):
        while self.last_applied < self.leader_commit:
            if self.last_applied+1 >= len(self.log):
                break

            self.last_applied = self.last_applied + 1
            entry = self.log[self.last_applied]["entry"]

            op, key, value = entry[0], entry[1], entry[2]

            if op == "SET":
                self.state_machine[key] = value
            elif op == "DELETE" and key in self.state_machine:
                del self.state_machine[key] 
            elif op == "GET":
                self.logger.set(f"GET {key}={self.state_machine.get(key, 'not found')}")
       
        self.logger.set_state_machine(self.state_machine, self.id)
        #self.logger.set(f"[{self.id}] ステートマシン　=　{self.state_machine}")


    # def keep_listening(self):
    #     server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #     server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    #     server_sock.bind(("127.0.0.1", self.id))
    #     server_sock.listen()

    #     self.logger.set(f"[{self.id}] listening...")

    #     while True:
    #         client_sock, addr = server_sock.accept()
    #         t = threading.Thread(
    #             target=self.handle_connection,
    #             args=(client_sock,)
    #         )
    #         t.daemon = True
    #         t.start()


    def keep_listening(self):
        #ソケット通信でデータを受信する
        #データを受信したら別スレッドでhandle_messageを呼び出す
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("127.0.0.1", self.id))
        sock.listen()

        while True:
            client_sock, sender_addr = sock.accept()
            data = client_sock.recv(65536)
            client_sock.close()
            message = json.loads(data.decode("UTF-8"))
            t = threading.Thread(target=self.handle_message, args=(message,))
            t.start()


    # def send_message(self, target_port, message):
    #     with self.conn_lock:
    #         sock = self.connections.get(target_port)
    #         if sock is None:
    #             try:
    #                 sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #                 sock.connect(("127.0.0.1", target_port))
    #                 self.connections[target_port] = sock
    #             except ConnectionRefusedError:
    #                 return
    #         try:
    #             json_message = json.dumps(message).encode("UTF-8")
    #             length = len(json_message)
    #             sock.sendall(length.to_bytes(4,"big") + json_message)
    #         except (BrokenPipeError, ConnectionResetError):
    #             try:
    #                 sock.close()
    #             except:
    #                 pass
    #             del self.connections[target_port]


    def send_message(self, target_port, message):
        #ソケット通信でデータを送信する
        #メッセージにメッセージタイプを付与することで受信側が handle_message() で識別できるようにする

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect(("127.0.0.1", target_port))
            json_message = json.dumps(message).encode("UTF-8")
            sock.send(json_message)
        except ConnectionRefusedError:
        #     self.logger.set(f"[Process {self.id}] Process {target_port} is not available.")
            pass
        finally:
            sock.close()

    # def handle_connection(self, sock):
    #     buffer = b""

    #     with sock:
    #         while True:
    #             try:
    #                 data = sock.recv(4096)
    #                 if not data:
    #                     break
    #                 buffer += data
    #                 while True:
    #                     if len(buffer) < 4:
    #                         break
    #                     msg_len = int.from_bytes(buffer[:4], "big")
    #                     if len(buffer) < 4 + msg_len:
    #                         break
    #                     msg = buffer[4:4+msg_len]
    #                     buffer = buffer[4+msg_len:]
    #                     message = json.loads(msg.decode("UTF-8"))
    #                     self.handle_message(message)
    #             except (ConnectionResetError, json.JSONDecodeError):
    #                 break

    #     self.logger.set(f"[{self.id}] connection closed")



    def handle_message(self, message):
        #keep_listeningから呼ばれ、受信したメッセージを処理する
        #メッセージタイプに応じて別の処理を行う
        message_type = message.get("type")

        if message_type == "APPEND_ENTRIES":
            message_term = message.get("term")
            message_leaderid = message.get("leaderID")
            message_previndex = message.get("prevLogIndex")
            message_prevterm = message.get("prevLogTerm")
            message_entries = message.get("entries")#差分を受け取り
            message_leadercommit = message.get("leaderCommit")

            # self.logger.set(f"ターム: {message_term}")
            # self.logger.set(f"リーダーID: {message_leaderid}")
            # self.logger.set(f"受信エントリ: {message_entries}")

            self.on_append_entries(
                term = message_term,
                leader_id = message_leaderid,
                prev_log_index = message_previndex,
                prev_log_term = message_prevterm,
                entries = message_entries,
                leader_commit = message_leadercommit,
            )

        if message_type == "RESPONSE":
            message_success = message.get("success")
            message_from_id = message.get("from_id")
            self.on_append_entries_response(
                from_id = message_from_id,
                success = message_success,
            )





    def run(self):
        self.logger.set(f"[Process {self.id}] 起動しました。")
        #個別スレッドとしてソケット通信を待つkeep_listeningを起動
        listener_thread = threading.Thread(target=self.keep_listening)
        listener_thread.daemon = True
        listener_thread.start()

        while True:
            if self.id == 10001:
                self.is_leader = True

            if self.is_leader and not self.next_index:
                for pid in self.all_process_ids:
                    if pid != self.id:
                        self.next_index[pid] = 0
                        #self.logger.set("nextIndex初期化" + str(self.next_index))

            if self.is_leader and not self.match_index:
                for k in self.all_process_ids:
                    if k != self.id:
                        self.match_index[k] = -1
                        #self.logger.set("matchIndex初期化")

            if self.is_leader == True:
                self.append_entries()
                self.logger.set("自分がリーダー")
                self.logger.set(self.log)
                time.sleep(2)


    

       


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
        0,
        0
        )

    #コマンド入力用のスレッド
    input_thread = threading.Thread(target=p.input_logs)
    input_thread.daemon = True
    input_thread.start()
    p.run()
