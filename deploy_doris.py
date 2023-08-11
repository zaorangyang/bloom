import fabric
import os
import shutil
import argparse

def get_ip_cird(ip):
    shards = ip.split(".")
    shards[-1] = "0"
    return ".".join(shards)

def install_fe(fe_ip, username, password, tarball):
    conn = fabric.Connection(host=fe_ip, user=username, connect_kwargs={"password": password})  
    result = conn.sudo("sudo yum install -y java-1.8.0-openjdk.x86_64", hide=True)
    print(f"Execution on %s:\n{result.stdout}" % fe_ip)
    conn.put(tarball, "/home/disk1/sr/")
    conn.run("tar xvf /home/disk1/sr/%s" % tarball)    
    cur_dir = os.path.dirname(os.path.abspath(__file__)) + "/"
    shutil.copy(cur_dir+"doris/fe.conf", "./fe.conf")
    cmd = "sed -i 's/$NETWORKS/%s/g' fe.conf" % get_ip_cird(fe_ip)
    os.system(cmd)
    conn.put("fe.conf", "/home/disk1/sr/%s/fe/conf" % tarball.split(".tar")[0])
    conn.run("/home/disk1/sr/%s/fe/bin/start_fe.sh --daemon" % tarball.split(".tar")[0])
    os.system("mysql -uroot -P9030 -h %s -e 'show frontends\G'" % fe_ip)
    
def install_be(be_ips, username, password, tarball, fe_ip):
    for ip in be_ips:
        conn = fabric.Connection(host=ip, user=username, connect_kwargs={"password": password})  
        result = conn.sudo("sudo yum install -y java-1.8.0-openjdk.x86_64", hide=True)
        print(f"Execution on %s:\n{result.stdout}" % ip)
        conn.put(tarball, "/home/disk1/sr/")
        conn.run("tar xvf /home/disk1/sr/%s" % tarball)    
        cur_dir = os.path.dirname(os.path.abspath(__file__)) + "/"
        shutil.copy(cur_dir+"doris/be.conf", "./be.conf")
        cmd = "sed -i 's/$NETWORKS/%s/g' be.conf" % get_ip_cird(ip)
        os.system(cmd)
        conn.put("be.conf", "/home/disk1/sr/%s/be/conf" % tarball.split(".tar")[0])
        conn.sudo("sudo sysctl -w vm.max_map_count=2000000", hide=True)
        conn.sudo("sudo sed -i 's/65535/555350/g' /etc/security/limits.conf", hide=True)
        conn.sudo("sudo mkdir -p /home/disk1/data", hide=True)
        conn.sudo("sudo chown -R sr /home/disk1/data", hide=True)        
        conn.sudo("sudo mkdir -p /home/disk2/data", hide=True)
        conn.sudo("sudo chown -R sr /home/disk2/data", hide=True)
        conn.close()
        conn = fabric.Connection(host=ip, user=username, connect_kwargs={"password": password})
        conn.run("/home/disk1/sr/%s/be/bin/start_be.sh --daemon" % tarball.split(".tar")[0])
        os.system("mysql -uroot -P9030 -h %s -e 'ALTER SYSTEM ADD BACKEND \"%s:%s\"'" % (fe_ip, ip, "9050"))
    os.system("mysql -uroot -P9030 -h %s -e 'show frontends\G'" % fe_ip)
    os.system("mysql -uroot -P9030 -h %s -e 'show backends\G'" % fe_ip)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Deploy doris.')
    parser.add_argument('-be', required=True, help='Specify the beaddress')
    parser.add_argument('-fe', required=True, help='Specify the fe address')
    parser.add_argument('-tarball', required=True, help='Specify doris tarball path')
    parser.add_argument('-u', default='sr', help='Specify the user (default: sr)')
    parser.add_argument('-p', default='sr@test', help='Specify the password (default: sr@test)')
    
    args = parser.parse_args()
    be_ips = args.be.split(',')
    fe_ip = args.fe
    username = args.u
    password = args.p
    tarball = args.tarball
    install_fe(fe_ip, username, password, tarball)
    install_be(be_ips, username, password, tarball, fe_ip)