import fabric
import os
import shutil
import argparse

def install_dependency(ips, username, password):
    for ip in ips:
        try:
            # 要确保ips的ip已经添加到本机的known_lists，否则会出现连接超时
            conn = fabric.Connection(host=ip, user=username, connect_kwargs={"password": password})    
            shell_script = """
            export https_proxy=https://172.26.92.139:28888
            wget https://archive.apache.org/dist/kafka/3.3.2/kafka_2.13-3.3.2.tgz
            tar xzvf kafka_2.13-3.3.2.tgz
            wget https://dlcdn.apache.org/zookeeper/zookeeper-3.7.1/apache-zookeeper-3.7.1-bin.tar.gz
            tar xzvf apache-zookeeper-3.7.1-bin.tar.gz
            """
            result = conn.sudo(shell_script, hide=True)
            print(f"Execution on {ip}:\n{result.stdout}")
            print("install dependency on {ip} successfully")
        except Exception as e:
            print(f"Failed to execute on {ip}. Error: {e}")
            exit(-1)
    print("install dependency successfully")

def install_zk(ips, username, password):
    # 上传conf文件
    cur_dir = os.path.dirname(os.path.abspath(__file__)) + "/"
    content = ""
    with open(cur_dir + "kafka/zoo.cfg", 'r') as file:
        content = file.read()
    for i in range(len(ips)):
        content = content + "server." + str(i+1) + "=" + ips[i] + ":2888:3888\n"
    with open("zoo.cfg", 'w') as file:
        file.write(content)
    for i in range(len(ips)):
        ip = ips[i]
        try:
            conn = fabric.Connection(host=ip, user=username, connect_kwargs={"password": password})
            conn.put("zoo.cfg", "/home/disk1/sr/apache-zookeeper-3.7.1-bin/conf")
            print("zoo.cfg upload successfully")
            conn.run("mkdir -p /tmp/zookeeper")
            conn.run("echo " + str(i+1) + " > /tmp/zookeeper/myid")
            result = conn.run("JAVA_HOME=/usr/lib/jvm/java-1.8.0 /home/disk1/sr/apache-zookeeper-3.7.1-bin/bin/zkServer.sh start")
            print(f"Execution on {ip}:\n{result.stdout}")
            print("install zk on {ip} successfully", ip)
        except Exception as e:
            print(f"zookeeper run failed: {e}")
            exit(-1)
    print("install zk successfully")
    
    
def install_kafka(ips, username, password):
    # 上传conf文件
    cur_dir = os.path.dirname(os.path.abspath(__file__)) + "/"
    content = ""
    zk_connect = ""
    for ip in ips:
        zk_connect = zk_connect + ip + ":2181,"
    zk_connect = zk_connect[:-1]
    print(zk_connect)
    for i in range(len(ips)):
        with open(cur_dir + "kafka/server.properties", 'r') as file:
            content = file.read()
        content = content + "listeners=PLAINTEXT://%s:9092\n" % ips[i]
        content = content + "broker.id=" + str(i+1) +"\n"
        content = content + "log.dirs=/home/disk1/kafka-data,/home/disk2/kafka-data\n"
        content = content + "zookeeper.connect=" + zk_connect + "\n"
        with open("server.properties", 'w') as file:
            file.write(content)   
        ip = ips[i]
        try:
            conn = fabric.Connection(host=ip, user=username, connect_kwargs={"password": password})
            conn.put("server.properties", "/home/disk1/sr/kafka_2.13-3.3.2/config")
            print("server.properties upload successfully")
            conn.sudo("sudo mkdir -p /home/disk1/kafka-data", hide=True)
            conn.sudo("sudo chown -R sr /home/disk1/kafka-data", hide=True)        
            conn.sudo("sudo mkdir -p /home/disk2/kafka-data", hide=True)
            conn.sudo("sudo chown -R sr /home/disk2/kafka-data", hide=True)
            # kafka-server-start.sh
            result = conn.run("JAVA_HOME=/usr/lib/jvm/java-1.8.0 /home/disk1/sr/kafka_2.13-3.3.2/bin/kafka-server-start.sh -daemon /home/disk1/sr/kafka_2.13-3.3.2/config/server.properties")
            print(f"Execution on {ip}:\n{result.stdout}")
            print("run kafka on {ip} successfully", ip)
        except Exception as e:
            print(f"kafka run failed: {e}")
            exit(-1)
    print("install kafka successfully")
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Deploy clichouse.')
    parser.add_argument('-addr', required=True, help='Specify the address')
    parser.add_argument('-u', default='sr', help='Specify the user (default: sr)')
    parser.add_argument('-p', default='sr@test', help='Specify the password (default: sr@test)')
    args = parser.parse_args()
    ips = args.addr.split(',')
    username = args.u
    password = args.p
    install_dependency(ips, username, password)
    install_zk(ips, username, password)
    install_kafka(ips, username, password)
    
