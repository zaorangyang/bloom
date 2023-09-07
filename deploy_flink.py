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

def install_flink(ips, username, password, tarball):
    master = ips[0]
    cur_dir = os.path.dirname(os.path.abspath(__file__)) + "/"
    with open("workers", "w") as file:
        for ip in ips:    
            file.write(ip + "\n")
    flink_dir = tarball.split(".tar")[0]
    for ip in ips:
        conn = fabric.Connection(host=ip, user=username, connect_kwargs={"password": password})  
        conn.put(tarball, "/home/disk1/sr/")
        conn.run("tar xvf /home/disk1/sr/%s" % tarball)
        shutil.copy(cur_dir+"flink/flink-conf.yaml", "./flink-conf.yaml")
        replace_line_in_file("flink-conf.yaml", "jobmanager.rpc.address: ", "jobmanager.rpc.address: %s" % master)
        replace_line_in_file("flink-conf.yaml", "taskmanager.host: ", "taskmanager.host: %s" % ip)
        conn.put("workers", "/home/disk1/sr/%s/conf" % flink_dir)
        conn.put("flink-conf.yaml", "/home/disk1/sr/%s/conf" % flink_dir)
    conn = fabric.Connection(host=master, user=username, connect_kwargs={"password": password})  
    conn.run("JAVA_HOME=/usr/lib/jvm/java-1.8.0 /home/disk1/sr/%s/bin/start-cluster.sh" % flink_dir)
    print("install flink successfully, master nonde is %s" % master) 
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Deploy flink.')
    parser.add_argument('-addr', required=True, help='Specify the address')
    parser.add_argument('-tarball', required=True, help='Specify the flink tarball')
    parser.add_argument('-u', default='sr', help='Specify the user (default: sr)')
    parser.add_argument('-p', default='sr@test', help='Specify the password (default: sr@test)')
    args = parser.parse_args()
    ips = args.addr.split(',')
    tarball = args.tarball
    username = args.u
    password = args.p
    install_flink(ips, username, password, tarball)

