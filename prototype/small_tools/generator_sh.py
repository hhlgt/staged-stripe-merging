import os

current_path = os.getcwd()
parent_path = os.path.dirname(current_path)
cluster_number = 12
datanode_number_per_cluster = 10
datanode_port_start = 17600
cluster_id_start = 0
iftest = False

proxy_ip_list = [
    ["10.0.0.2",50405],
    ["10.0.0.3",50405],
    ["10.0.0.5",50405],
    ["10.0.0.6",50405],
    ["10.0.0.7",50405],
    ["10.0.0.8",50405],
    ["10.0.0.9",50405],
    ["10.0.0.10",50405],
    ["10.0.0.11",50405],
    ["10.0.0.12",50405],
    ["10.0.0.13",50405],
    ["10.0.0.14",50405],
    ["10.0.0.15",50405],
    ["10.0.0.16",50405],
    ["10.0.0.17",50405],
    ["10.0.0.18",50405]
]
coordinator_ip = "10.0.0.4"

proxy_num = len(proxy_ip_list)

#cluster_informtion = {cluster_id:{'proxy':0.0.0.0:50005,'datanode':[[ip,port],...]},}
cluster_informtion = {}
def generate_cluster_info_dict():
    for i in range(proxy_num):
        new_cluster = {}
        
        new_cluster["proxy"] = proxy_ip_list[i][0]+":"+str(proxy_ip_list[i][1])
        datanode_list = []
        for j in range(datanode_number_per_cluster):
            port = datanode_port_start + j
            if iftest:
                port = datanode_port_start + i*100 + j
            datanode_list.append([proxy_ip_list[i][0], port])
        new_cluster["datanode"] = datanode_list
        cluster_informtion[i] = new_cluster
            
def generate_run_proxy_datanode_file():
    file_name = parent_path + '/run_proxy_datanode.sh'
    with open(file_name, 'w') as f:
        f.write("pkill -9 run_datanode\n")
        f.write("pkill -9 run_proxy\n")
        f.write("\n")
        for cluster_id in cluster_informtion.keys():
            print("cluster_id",cluster_id)
            for each_datanode in cluster_informtion[cluster_id]["datanode"]:
                f.write("./project/cmake/build/run_datanode "+str(each_datanode[0])+":"+str(each_datanode[1])+"\n")
            f.write("\n") 
        for proxy_ip_port in proxy_ip_list:
            f.write("./project/cmake/build/run_proxy "+str(proxy_ip_port[0])+":"+str(proxy_ip_port[1])+"\n")   
        f.write("\n")

def generater_cluster_information_xml():
    file_name = parent_path + '/project/config/clusterInformation.xml'
    import xml.etree.ElementTree as ET
    root = ET.Element('clusters')
    root.text = "\n\t"
    for cluster_id in cluster_informtion.keys():
        cluster = ET.SubElement(root, 'cluster', {'id': str(cluster_id), 'proxy': cluster_informtion[cluster_id]["proxy"]})
        cluster.text = "\n\t\t"
        datanodes = ET.SubElement(cluster, 'datanodes')
        datanodes.text = "\n\t\t\t"
        for index,each_datanode in enumerate(cluster_informtion[cluster_id]["datanode"]):
            datanode = ET.SubElement(datanodes, 'datanode', {'uri': str(each_datanode[0])+":"+str(each_datanode[1])})
            #datanode.text = '\n\t\t\t'
            if index == len(cluster_informtion[cluster_id]["datanode"]) - 1:
                datanode.tail = '\n\t\t'
            else:
                datanode.tail = '\n\t\t\t'
        datanodes.tail = '\n\t'
        if cluster_id == len(cluster_informtion)-1:
            cluster.tail = '\n'
        else:
            cluster.tail = '\n\t'
    #root.tail = '\n'
    tree = ET.ElementTree(root)
    tree.write(file_name, encoding="utf-8", xml_declaration=True)

def test_chat_gpt():
    import xml.etree.ElementTree as ET
    root = ET.Element('clusters')
    cluster = ET.SubElement(root, 'cluster', {'id': '0', 'proxy': '0.0.0.0:50005'})
    datanodes = ET.SubElement(cluster, 'datanodes')
    for i in range(9000, 9020):
        datanode = ET.SubElement(datanodes, 'datanode', {'uri': '0.0.0.0:{}'.format(i)})
        datanode.tail = '\n\t\t'
    datanodes.tail = '\n\t'
    cluster.tail = '\n'
    tree = ET.ElementTree(root)
    tree.write('clusters1.xml', encoding='utf-8', xml_declaration=True)
            
def cluster_generate_run_proxy_datanode_file(ip, port, i):
    file_name = parent_path + '/run_cluster_sh/' + str(i) +'/cluster_run_proxy_datanode.sh'
    with open(file_name, 'w') as f:
        f.write("pkill -9 run_datanode\n")
        f.write("pkill -9 run_proxy\n")
        f.write("\n")
        for each_datanode in cluster_informtion[0]["datanode"]:
            f.write("./project/cmake/build/run_datanode "+ip+":"+str(each_datanode[1])+"\n")
        f.write("\n") 
        f.write("./project/cmake/build/run_proxy "+ip+":"+str(port)+" "+coordinator_ip+"\n")   
        f.write("\n")

if __name__ == "__main__":
    generate_cluster_info_dict()
    # print(cluster_informtion)
    # generate_run_proxy_datanode_file()
    generater_cluster_information_xml()
    cnt = 0
    for proxy in proxy_ip_list:
        cluster_generate_run_proxy_datanode_file(proxy[0], proxy[1], cnt)
        cnt += 1
    