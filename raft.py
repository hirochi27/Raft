import threading
import socket
import json
import time

class Process():
    def __init__(self, process_id, all_process_ids, prev_log_index, prev_log_term, leader_commit, next_index, append_entries_success ):
        super().__init__()
        self.id = process_id
        self.all_process_ids = all_process_ids  # 全プロセスIDのリスト
        self.is_leader = False
        
        self.current_term = 1
        self.leader_id = 10001  # リーダー固定
        self.prev_log_index = prev_log_index
        self.prev_log_term = prev_log_term
        self.entries = [] #フォロワーに送るエントリ
        self.leader_commit = leader_commit

        self.log = [] #持ってるログ（ターム、エントリ）
        self.next_index = {
            10001: 0,#0を最新のリーダーのエントリの次にしたい
            10002: 0,
            10003: 0
        }
        
        self.append_entries_success = False
        #for pid in all_process_ids:
          #self.next_index[pid] = 0

    def input_logs(self):
        #別スレッドで入力を受け取る
        #自身のログに追加
        #currentIndexとprevIndex,Termを抽出
        while True:
            entry = input("enter command to add")

            log_entry = {
                "term" : self.current_term,
                "entry" : entry
            }
            
            self.log.append(log_entry)#リーダーのログに追加
            print(str(entry) + "をリーダーのログに追加しました")
            print(str(self.log) + "現在のログ")

    def output_entries(self):
        pass

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
            #if id == self.leader_id:
                #continue
            self.entries = self.log[self.next_index[id]:]#送るエントリは、logのnextindexから先
            self.prev_log_index = self.next_index[id]-1 #prevlogindexはnextの一個前
            if self.prev_log_index == -1:#prevlogindexが-1のときprevlogrermは１
                self.prev_log_term = 1 #ターム数の変更実装前なのでとりあえず１
            elif 0 <= self.prev_log_index < len(self.log):
                self.prev_log_term = self.log[self.prev_log_index]["term"]
            else:
                print(f"Warning: prev_log_index {self.prev_log_index} is out of range for log length {len(self.log)}")
                self.prev_log_term = 1
                #logの中の、インデックス＝prevlogindexに含まれるキーtermの値を取得
            
            #フォロワーから返信受け取ってから以下のプリント
            #print("prevlogインデックス"+str(self.#prev_log_index)+"prevlogターム"+ str(self.prev_log_term))

            self.send_message(id, message_append_entries)
        print("AppendEntriesRPC")
        time.sleep(1.0)


    def on_append_entries(self, term,  leader_id, prev_log_index, prev_log_term, entries, leader_commit):
        #appendEintriesを受け取ったフォロワー側の処理
        #prevlogindex,prevlogtermを受け取ってOkの時はsuccessをTrue。一致しない時はFalse
        #これをリーダーへ返す
        response = {
            "type" : "RESPONSE",
            "from_id" : self.id,
            "success" :  self.append_entries_success,
            "entries_count": len(entries)
            }
        
        if prev_log_term < self.current_term:
            self.append_entries_success = False
            self.send_message(self.leader_id, response)
            print("False１を送りました")
        elif prev_log_index in self.log and prev_log_term in self.log:
            self.append_entries_success = False
            self.send_message(self.leader_id, response)
            print("False２を送りました")
        else: #self.prev_log_term >= self.current_term
            self.append_entries_success = True
            self.send_message(self.leader_id, response)
            print("Trueを送りました")
        print("!!self.append_entries_success!!")

    def on_append_entries_response(self, from_id, success, count):
        #true,Falseを受け取ったら
        #trueの場合、nextindexを”送ったエントリの次”にする
        print(success)
        if success == True:
            print("True")
            self.next_index[from_id] = self.next_index[from_id] + count#仮
            print(self.next_index)
        elif success == False:
            print("False")
            self.next_index[from_id] = max(0, self.next_index[from_id] -1)
            pass


    def keep_listening(self):
        #ソケット通信でデータを受信する
        #データを受信したら別スレッドでhandle_messageを呼び出す
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
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
            message_entries = message.get("entries")#差分を受け取り
            message_leadercommit = message.get("leaderCommit")

            print("ターム" + str(message_term))
            print("リーダー"+ str(message_leaderid)+str(message_entries))   

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
            message_entries_count = message.get("entries_count")
            self.on_append_entries_response(
                from_id = message_from_id,
                success = message_success,
                count = message_entries_count
            )

            

            

    def run(self):
        print(f"[Process {self.id}] 起動しました。")
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
                        print("nextIndex初期化" + str(self.next_index))

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
        0,
        0
        ) 
    
    #コマンド入力用のスレッド
    input_thread = threading.Thread(target=p.input_logs)
    input_thread.daemon = True
    input_thread.start()
    p.run()