import fabric
import os
import shutil
import argparse

def replace_line_in_file(filename, keyword, new_content):
    with open(filename, 'r') as file:
        lines = file.readlines()

    with open(filename, 'w') as file:
        for line in lines:
            if keyword in line:
                line = new_content + '\n'
            file.write(line)

def install_dependency(ips, username, password):
    for ip in ips:
        try:
            # 要确保ips的ip已经添加到本机的known_lists，否则会出现连接超时
            conn = fabric.Connection(host=ip, user=username, connect_kwargs={"password": password})    
            shell_script = """
            sudo yum-config-manager --add-repo https://packages.clickhouse.com/rpm/clickhouse.repo
            sudo yum install -y clickhouse-server clickhouse-client
            sudo yum install -y java-1.8.0-openjdk.x86_64
            export https_proxy=https://172.26.92.139:28888
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
    with open(cur_dir + "clickhouse/zoo.cfg", 'r') as file:
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
            result = conn.run("/home/disk1/sr/apache-zookeeper-3.7.1-bin/bin/zkServer.sh start")
            print(f"Execution on {ip}:\n{result.stdout}")
            print("install zk on {ip} successfully", ip)
        except Exception as e:
            print(f"zoo.cfg upload failed: {e}")
            exit(-1)
    print("install zk successfully")

# 3shard, one shard contains 3 replicas
def install_ck(ips, username, password):
    cur_dir = os.path.dirname(os.path.abspath(__file__)) + "/"
    shutil.copy(cur_dir+"clickhouse/config.xml", "./config.xml")
    ip_hosts = {}
    for i in range(len(ips)):
        ip = ips[i]
        conn = fabric.Connection(host=ip, user=username, connect_kwargs={"password": password})
        result = conn.run("hostname")
        hostname = result.stdout[:-1]
        replace_line_in_file("config.xml", "172.26.95." + str(5+i), "<host>" + hostname + "</host>")
        ip_hosts[ip] = hostname
        
    with open('hosts-ck', 'w') as hosts_file:
        for ip, hostname in ip_hosts.items():
            hosts_file.write(f'{ip} {hostname}\n')
    
    for i in range(len(ips)):
        ip = ips[i]
        conn = fabric.Connection(host=ip, user=username, connect_kwargs={"password": password})
        try: 
            conn.put("hosts-ck", "/home/disk1/sr/")
            conn.sudo("mv /home/disk1/sr/hosts-ck /etc/hosts")
        except Exception as e:
            print(f"alter hosts error on {ips[i]} failed: {e}")
            exit(-1)
    
    for i in range(len(ips)):
        ip = ips[i]
        try:
            conn = fabric.Connection(host=ip, user=username, connect_kwargs={"password": password})
            conn.sudo("chown -R clickhouse.clickhouse /etc/clickhouse-server", hide=True)
            make_dir = """
            sudo mkdir -p /home/disk1/ck1
            sudo chown -R clickhouse.clickhouse /home/disk1/ck1
            
            sudo mkdir -p /home/disk2/ck1
            sudo chown -R clickhouse.clickhouse /home/disk2/ck1
            
            sudo mkdir -p /home/disk1/ck2
            sudo chown -R clickhouse.clickhouse /home/disk1/ck2

            sudo mkdir -p /home/disk2/ck2
            sudo chown -R clickhouse.clickhouse /home/disk2/ck2

            sudo mkdir -p /home/disk1/ck3
            sudo chown -R clickhouse.clickhouse /home/disk1/ck3

            sudo mkdir -p /home/disk2/ck3
            sudo chown -R clickhouse.clickhouse /home/disk2/ck3
            """
            conn.sudo(make_dir, hide=True)
            
            shutil.copy("config.xml", "./config1.xml")
            replace_line_in_file("config1.xml", "<log>", "<log>/home/disk1/ck1/clickhouse-server.log</log>")
            replace_line_in_file("config1.xml", "<errorlog>", "<errorlog>/home/disk1/ck1/clickhouse-server.err.log</errorlog>")
            replace_line_in_file("config1.xml", "<http_port>", "<http_port>18123</http_port>")
            replace_line_in_file("config1.xml", "<tcp_port>", "<tcp_port>19000</tcp_port>")
            replace_line_in_file("config1.xml", "<mysql_port>", "<mysql_port>19004</mysql_port>")
            replace_line_in_file("config1.xml", "<postgresql_port>", "<postgresql_port>19005</postgresql_port>")
            replace_line_in_file("config1.xml", "<interserver_http_port>", "<interserver_http_port>19009</interserver_http_port>")
            replace_line_in_file("config1.xml", "<path>/home/disk1/ck1/</path>", "<path>/home/disk1/ck1/</path>")
            replace_line_in_file("config1.xml", "<path>/home/disk2/ck1/</path>", "<path>/home/disk2/ck1/</path>")
            replace_line_in_file("config1.xml", "<tmp_path>/home/disk1/ck1</tmp_path>", "<tmp_path>/home/disk1/ck1</tmp_path>")
            replace_line_in_file("config1.xml", "<user_files_path>", "<user_files_path>/home/disk1/ck1/user_files/</user_files_path>")
            replace_line_in_file("config1.xml", "<path>/home/disk1/ck1/access/</path>", "<path>/home/disk1/ck1/access/</path>")
            replace_line_in_file("config1.xml", "<shard>01</shard>", "<shard>01</shard>")
            replace_line_in_file("config1.xml", "<replica>cluster-01-01</replica>", "<replica>cluster-01-0" +str(i+1) + "</replica>")
            replace_line_in_file("config1.xml", "format_schema_path", "<format_schema_path>/home/disk1/ck1/format_schemas/</format_schema_path>")
            conn.put("config1.xml", "/home/disk1/sr")
            conn.sudo("mv /home/disk1/sr/config1.xml /etc/clickhouse-server")
            conn.sudo("sudo -u clickhouse clickhouse-server --config-file=/etc/clickhouse-server/config1.xml --daemon")
            
            shutil.copy("config.xml", "config2.xml")
            replace_line_in_file("config2.xml", "<log>", "<log>/home/disk1/ck2/clickhouse-server.log</log>")
            replace_line_in_file("config2.xml", "<errorlog>", "<errorlog>/home/disk1/ck2/clickhouse-server.err.log</errorlog>")
            replace_line_in_file("config2.xml", "<http_port>", "<http_port>28123</http_port>")
            replace_line_in_file("config2.xml", "<tcp_port>", "<tcp_port>29000</tcp_port>")
            replace_line_in_file("config2.xml", "<mysql_port>", "<mysql_port>29004</mysql_port>")
            replace_line_in_file("config2.xml", "<postgresql_port>", "<postgresql_port>29005</postgresql_port>")
            replace_line_in_file("config2.xml", "<interserver_http_port>", "<interserver_http_port>29009</interserver_http_port>")
            replace_line_in_file("config2.xml", "<path>/home/disk1/ck1/</path>", "<path>/home/disk1/ck2/</path>")
            replace_line_in_file("config2.xml", "<path>/home/disk2/ck1/</path>", "<path>/home/disk2/ck2/</path>")
            replace_line_in_file("config2.xml", "<tmp_path>/home/disk1/ck1</tmp_path>", "<tmp_path>/home/disk1/ck2</tmp_path>")
            replace_line_in_file("config2.xml", "<user_files_path>", "<user_files_path>/home/disk1/ck2/user_files/</user_files_path>")
            replace_line_in_file("config2.xml", "<path>/home/disk1/ck1/access/</path>", "<path>/home/disk1/ck2/access/</path>")
            replace_line_in_file("config2.xml", "<shard>01</shard>", "<shard>02</shard>")
            replace_line_in_file("config2.xml", "<replica>cluster-01-01</replica>", "<replica>cluster-02-0" + str(i+1) + "</replica>")
            replace_line_in_file("config2.xml", "format_schema_path", "<format_schema_path>/home/disk1/ck2/format_schemas/</format_schema_path>")
            conn.put("config2.xml", "/home/disk1/sr")
            conn.sudo("mv /home/disk1/sr/config2.xml /etc/clickhouse-server")
            conn.sudo("sudo -u clickhouse clickhouse-server --config-file=/etc/clickhouse-server/config2.xml --daemon")
            
            
            shutil.copy("config.xml", "config3.xml")
            replace_line_in_file("config3.xml", "<log>", "<log>/home/disk1/ck3/clickhouse-server.log</log>")
            replace_line_in_file("config3.xml", "<errorlog>", "<errorlog>/home/disk1/ck3/clickhouse-server.err.log</errorlog>")
            replace_line_in_file("config3.xml", "<http_port>", "<http_port>38123</http_port>")
            replace_line_in_file("config3.xml", "<tcp_port>", "<tcp_port>39000</tcp_port>")
            replace_line_in_file("config3.xml", "<mysql_port>", "<mysql_port>39004</mysql_port>")
            replace_line_in_file("config3.xml", "<postgresql_port>", "<postgresql_port>39005</postgresql_port>")
            replace_line_in_file("config3.xml", "<interserver_http_port>", "<interserver_http_port>39009</interserver_http_port>")
            replace_line_in_file("config3.xml", "<path>/home/disk1/ck1/</path>", "<path>/home/disk1/ck3/</path>")
            replace_line_in_file("config3.xml", "<path>/home/disk2/ck1/</path>", "<path>/home/disk2/ck3/</path>")
            replace_line_in_file("config3.xml", "<tmp_path>/home/disk1/ck1</tmp_path>", "<tmp_path>/home/disk1/ck3</tmp_path>")
            replace_line_in_file("config3.xml", "<user_files_path>", "<user_files_path>/home/disk1/ck3/user_files/</user_files_path>")
            replace_line_in_file("config3.xml", "<path>/home/disk1/ck1/access/</path>", "<path>/home/disk1/ck3/access/</path>")
            replace_line_in_file("config3.xml", "<shard>01</shard>", "<shard>03</shard>")
            replace_line_in_file("config3.xml", "<replica>cluster-01-01</replica>", "<replica>cluster-03-0" + str(i+1) + "</replica>")
            replace_line_in_file("config3.xml", "format_schema_path", "<format_schema_path>/home/disk1/ck3/format_schemas/</format_schema_path>")
            conn.put("config3.xml", "/home/disk1/sr")
            conn.sudo("mv /home/disk1/sr/config3.xml /etc/clickhouse-server")
            conn.sudo("sudo -u clickhouse clickhouse-server --config-file=/etc/clickhouse-server/config3.xml --daemon")

        except Exception as e:
            print(f"install ck on {ip} failed: {e}")
            exit(-1)
    print("install ck on successfully")
    print("cluster info is:")
    cmd = "mysql -h %s -P19004 -udefault -e 'SELECT * FROM system.clusters\G'" % ips[0]
    os.system(cmd)

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
    install_ck(ips, username, password)