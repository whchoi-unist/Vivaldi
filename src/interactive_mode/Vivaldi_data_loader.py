#!/home/whchoi/python
from mpi4py import MPI

# mpi init
parent = MPI.Comm.Get_parent()
parent = parent.Merge()
comm = parent
size = comm.Get_size()
rank = comm.Get_rank()
name = MPI.Get_processor_name()
	

# set environment variabled
import os, sys
VIVALDI_PATH = os.environ.get('vivaldi_path')
sys.path.append(VIVALDI_PATH+'/src')    


import numpy
from Vivaldi_misc import *
#from socket import *
import socket


def disconnect():
	print "Disconnect", rank, name
	comm.Disconnect()
	MPI.Finalize()
	exit()


def notice_file_exist(existance, dest):
	comm.send(existance,	dest=dest, 	tag=11)

def file_check(file_path, source):
	return os.path.exists(file_path)



def load_data_local(data_package):
	dp = data_package





flag_times = {}
for elem in ["finish","memcpy_p2p","data_check","log"]:
	flag_times[elem] = 0 # execution
##########################################################

flag = ''
while flag != "finish":
	while not comm.Iprobe(source=MPI.ANY_SOURCE, tag=5):
		time.sleep(0.001)

	source = comm.recv(source=MPI.ANY_SOURCE,    tag=5)
	flag = comm.recv(source=source,              tag=5)

	#print_red("FROM LOADER %s, %s"%(socket.gethostname(),flag))


	if flag == "finish":
		disconnect()

	elif flag == "data_check":
		file_path = comm.recv(source=source, 	tag=55)
		existance = os.path.exists(file_path)
		comm.send(existance, dest=source, tag=11)

	elif flag == "data_load":
		data_package = comm.recv(source=source, 	tag=52)
		file_path    = comm.recv(source=source, 	tag=55)
		location     = comm.recv(source=source, 	tag=55)

		#print_green("HEY %s"%str((file_path, location)))
		data = load_data_from_local(data_package, file_path, data_package.data_halo, location)
		comm.send(data, dest=source, 	tag=57)

	elif flag == "upload_to_hdfs":
		file_path        = comm.recv(source=source, 	tag=55)
		hdfs_file_path   = comm.recv(source=source, 	tag=55)

		os.system("hdfs dfs -appendToFile %s %s"%(file_path, hdfs_file_path))

		comm.send("done",	dest=source,	tag=11)

		
		

	
