import fabric
import os
import shutil
import argparse

def install_node(nodes, username, password):
    for node in nodes:
        try:
            # 要确保ips的ip已经添加到本机的known_lists，否则会出现连接超时
            conn = fabric.Connection(host=node, user=username, connect_kwargs={"password": password})    
            shell_script = """
            export https_proxy=https://172.26.92.139:28888
            wget https://github.com/prometheus/node_exporter/releases/download/v1.6.1/node_exporter-1.6.1.linux-amd64.tar.gz
            tar xzvf node_exporter-1.6.1.linux-amd64.tar.gz
            cd node_exporter-1.6.1.linux-amd64
            nohup ./node_exporter > out.txt 2>&1 &
            """
            result = conn.sudo(shell_script, hide=True)
            print(f"Execution on {node}:\n{result.stdout}")
            print("install dependency on %s successfully" % node)
        except Exception as e:
            print(f"Failed to execute on %s. Error: {e}" % node)
            exit(-1)
    print("install node successfully")
    
def install_server(server, nodes, username, password):
    # https://github.com/prometheus/prometheus/releases/download/v2.45.0/prometheus-2.45.0.linux-amd64.tar.gz
    try:
        # 要确保ips的ip已经添加到本机的known_lists，否则会出现连接超时
        conn = fabric.Connection(host=server, user=username, connect_kwargs={"password": password})    
        shell_script = """
        export https_proxy=https://172.26.92.139:28888
        wget https://github.com/prometheus/prometheus/releases/download/v2.45.0/prometheus-2.45.0.linux-amd64.tar.gz
        tar xzvf prometheus-2.45.0.linux-amd64.tar.gz
        """
        result = conn.run(shell_script)
        print(f"Execution on {server}:\n{result.stdout}")
        print("install server on %s successfully" % server)
    except Exception as e:
        print(f"Failed to execute on %s. Error: {e}" % server)
        exit(-1)
    cur_dir = os.path.dirname(os.path.abspath(__file__)) + "/"
    shutil.copy(cur_dir+"prometheus/prometheus.yml", "./prometheus.yml")
    for node in nodes:
        job = """
  - job_name: '%s'
    static_configs:
      - targets: ["%s:9100"]
""" % (node, node)
        with open('prometheus.yml', "a") as file:
            for line in job:
                file.write(line)

    conn = fabric.Connection(host=server, user=username, connect_kwargs={"password": password})    
    conn.put("prometheus.yml", "/home/disk1/sr/prometheus-2.45.0.linux-amd64")
    shell_script = """
    cd /home/disk1/sr/prometheus-2.45.0.linux-amd64
    nohup ./prometheus --web.enable-admin-api --config.file=prometheus.yml > out.txt 2>&1 &
    """
    result = conn.run(shell_script, )
    print(f"Execution on {server}:\n{result.stdout}")
    

def install_grafana(server, username, password):
    try:
        # 要确保ips的ip已经添加到本机的known_lists，否则会出现连接超时
        conn = fabric.Connection(host=server, user=username, connect_kwargs={"password": password})
        shell_script = """
        export https_proxy=https://172.26.92.139:28888
        wget https://dl.grafana.com/enterprise/release/grafana-enterprise-9.0.3.linux-amd64.tar.gz
        tar xzvf grafana-enterprise-9.0.3.linux-amd64.tar.gz
        """
        result = conn.run(shell_script)
        print(f"Execution on {server}:\n{result.stdout}")
        print("install grafana on %s successfully" % server)
    except Exception as e:
        print(f"Failed to execute on %s. Error: {e}" % server)
        exit(-1)
        
    conn = fabric.Connection(host=server, user=username, connect_kwargs={"password": password})    
    shell_script = """
    cd /home/disk1/sr/grafana-9.0.3
    nohup ./bin/grafana-server --homepath ./ web > out.txt 2>&1 &
    """
    result = conn.run(shell_script, )
    print(f"Execution on {server}:\n{result.stdout}")
        
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Deploy prometheus.')
    parser.add_argument('-server', required=True, help='Specify the prometheus server address')
    parser.add_argument('-node', required=True, help='Specify the node address')
    parser.add_argument('-u', default='sr', help='Specify the user (default: sr)')
    parser.add_argument('-p', default='sr@test', help='Specify the password (default: sr@test)')
    
    args = parser.parse_args()
    nodes = args.node.split(',')
    server_ip = args.server
    username = args.u
    password = args.p
    # install_node(nodes, username, password)
    # install_server(server_ip, nodes, username, password)
    # install_grafana(server_ip, username, password)
    print("start successfully, please enter: ssh -NL 3000:%s:3000 yangzaorang@39.103.134.93 in your mac!" % server_ip)