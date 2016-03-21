#!/home/whchoi/python
from mpi4py import MPI

# mpi init
parent = MPI.Comm.Get_parent()
parent = parent.Merge()
comm = parent
size = comm.Get_size()
rank = comm.Get_rank()
name = MPI.Get_processor_name()
	
import pycuda.driver as cuda
from pycuda.compiler import SourceModule
from Vivaldi_misc import *
#from Vivaldi_reader import *
from texture import *
from numpy.random import rand
from Vivaldi_memory_packages import Data_package
import traceback
import socket
from hdfs import InsecureClient

import os, sys
VIVALDI_PATH = os.environ.get('vivaldi_path')
sys.path.append(VIVALDI_PATH+'/src')    
sys.path.append(VIVALDI_PATH+'/src/translator')    



def disconnect():
	print "Disconnect", rank, name
	comm.Disconnect()
	MPI.Finalize()
	exit()

# MPI Communication
#def wait_data_arrive(data_package, stream=None, vivaldi_stream=False):
# FREYJA STREAMING
def wait_data_arrive_streaming(data_package, streaming_cnt, stream=None, gmem_set=False):
	# local_functions
	#################################################################
	def prepare_dest_package_and_dest_devptr(is_new_data, dest_package):
		# ???????????????????
		output_package = dest_package.copy()
		if is_new_data:
			# create new data
			# because we don't have cuda memory allocation for dest_package
			dest_devptr, new_usage = malloc_with_swap_out(output_package.data_bytes)
			output_package.set_usage(new_usage)
			
			#cuda.memset_d8(dest_devptr, 0, output_package.data_bytes)
		else:
			# we already have cuda memory allocation
			# if there are enough halo, we can use exist buffer instead dest_package
			# if there are not enough halo, we have to allocate new buffer
			
			is_new_data_halo = task.dest.data_halo
			exist_data_halo = data_list[u][ss][sp].data_halo
			
			if is_new_data_halo <= exist_data_halo: 
				output_package = data_list[u][ss][sp]
			else:
				output_package = dest_package
			dest_devptr = data_list[u][ss][sp].devptr

		return output_package, dest_devptr
	def check_new_data(data_exist, dest_devptr):
		is_new_data = False
		if not data_exist: # data is not exist
			is_new_data = True
		else:
			# buffer not exist
			if dest_devptr == None: 
				is_new_data = True
		return is_new_data

	# data : input data
	# data_package : Data_package to be written
	# ori_split_position : original data split position
	def split_data_in_cpu(data, data_package, ori_split_position):
		data_range = data_package.data_range
		streaming_unit = data_package.disk_file_size#/data_package.disk_block_size
		if type(ori_split_position) == str:
			ori_split_position = eval(ori_split_position)
		data_halo = data_package.data_halo
		copy_str = 'data['
		for axis in ['z','y','x']:
			cur_range = data_range[axis]
			cur_start = int(ori_split_position[axis])-1
			if cur_start == 0:
				start = cur_range[0]-cur_start*streaming_unit
				end   = cur_range[1]-cur_start*streaming_unit
			else:
				start = cur_range[0]-cur_start*streaming_unit + data_halo
				end   = cur_range[1]-cur_start*streaming_unit + data_halo
			copy_str += '%s:%s,'%(str(start), str(end))
		copy_str = copy_str[:-1]+ ']'
		ret_data = eval(copy_str)
		return eval(copy_str)
		
	
	# initialize variables
	#################################################################
	u, ss, sp = data_package.get_id()
	
	# check recv_list
	flag = True
	if u not in recv_list: flag = False
	elif ss not in recv_list[u]: flag = False
	elif sp not in recv_list[u][ss]: flag = False

	# implementation
	#####################################################################

	if flag:
		# initialize variable
		# prepare copy
		# make dest data
		first = recv_list[u][ss][sp][0]
		dest_package = first['task'].source

		if dest_package.devptr == None:
			#if data_package.data_halo == 0:
			if True:
				stream_position = eval(sp)['z']-1
				hdfs_str  = data_package.stream_hdfs_file_name
				hdfs_addr = hdfs_str[:hdfs_str.rfind('/')]
				hdfs_path = hdfs_str[hdfs_str.rfind('/')+1:]
	
				# for local data load
				#data_package.file_name += data_package.stream_file_list[stream_position]
				#data_package.file_name = '/local/whchoi/data/stream/'+data_package.stream_file_list[stream_position]
	
				#print data_package.info()
				#print u, ss, sp, device_number
	  			
				import time
				t_before = time.time()
				data = load_from_hdfs(data_package, hdfs_addr, hdfs_path)
	   			#print "END TO LOAD DATA from %d th sequence"%(stream_position)
				t_diff = time.time() - t_before

					
					
			data_list[u][ss][sp].data = data
			data_list[u][ss][sp].devptr = data
	   		#data_package.data = data


		dest_package.split_data_range_streaming(streaming_cnt)

		du, dss, dsp = dest_package.get_id()


		# check data exist
		dest_devptr = None
		data_exist = False
		usage = 0

		if du in data_list:
			if dss in data_list[du]:
				if dsp in data_list[du][dss]:
					dest_devptr = data_list[du][dss][dsp].devptr
					usage = data_list[du][dss][dsp].data_bytes
					data_exist = True

		# data exist check
		#is_new_data = check_new_data(data_exist, dest_devptr)
		
		# prepare dest_package and dest_devptr
		#dest_package, dest_devptr = prepare_dest_package_and_dest_devptr(is_new_data, dest_package)
		
		dest_data_range = dest_package.data_range
		dest_buffer_range = dest_package.buffer_range

	
		# write from temp data to dest data
		for elem in recv_list[u][ss][sp]:
			# wait data arrive

			#MPI.Request.Wait(elem['request'])
			t_data = elem['data']

			t_devptr = t_data
			elem['t_devptr'] = t_devptr
				
			
			target = (u,ss,sp)
			if target not in gpu_list:
				gpu_list.append((u,ss,sp))

	
		#stream.synchronize()

		dest_package.memory_type = numpy.ndarray
		dest_package.data_dtype = numpy.ndarray
		dest_package.devptr = dest_devptr
		dest_package.data = t_data

		if not data_exist:
			if du not in data_list: data_list[du] = {}
			if dss not in data_list[du]: data_list[du][dss] = {}
		data_list[du][dss][dsp] = dest_package

		del recv_list[u][ss][sp]
		if recv_list[u][ss] == {}: del recv_list[u][ss]
		if recv_list[u] == {}: del recv_list[u]


	else:
		# data already in here

		target_dp = data_list[u][ss][sp]
		

		# prepare copy
		#refresh dest_package
		#data_package = data_package.copy()
		data_package.split_data_range_streaming(streaming_cnt)


		du, dss, dsp = data_package.get_id()

		# check data exist
		data_devptr = None
		data_exist = False
		usage = 0

		if du in data_list:
			if dss in data_list[du]:
				if dsp in data_list[du][dss]:
					data_devptr = data_list[du][dss][dsp].devptr
					usage = data_list[du][dss][dsp].data_bytes
					data_exist = True

		# data exist check
		is_new_data = check_new_data(data_exist, data_devptr)
		
		# prepare dest_package and dest_devptr
		data_package, data_devptr = prepare_dest_package_and_dest_devptr(is_new_data, data_package)
		
		data_data_range = data_package.data_range
		data_buffer_range = data_package.buffer_range

		t_data = split_data_in_cpu(target_dp.data, data_package, target_dp.split_position)

		
		if type(t_data) == numpy.ndarray and gmem_set:
			t_devptr, usage = malloc_with_swap_out(data_package.data_bytes)
			#print t_data[0][0], t_devptr, gmem_set
			# def copy rectangular
			cuda.memcpy_htod_async(t_devptr, t_data, stream=stream)
			#print "COPIED",t_devptr, data_package.get_id()
			#print data_package.info(), data_package.memory_type
			#elem['t_devptr'] = t_devptr
			#elem['usage'] = usage	
			del(t_data)
		else:
			t_devptr = t_data
			#elem['t_devptr'] = t_devptr
			
		target = (u,ss,sp)
		if target not in gpu_list:
			gpu_list.append((u,ss,sp))

		data_package.memory_type = 'devptr'
		data_package.data_dtype = 'cuda_memory'
		data_package.devptr = t_devptr
		#print 'WRITING', data_package, du, dss, dsp

		if not data_exist:
			if du not in data_list: data_list[du] = {}
			if dss not in data_list[du]: data_list[du][dss] = {}
		data_list[du][dss][dsp] = data_package
		#print data_list[du][dss][dsp]
		#print "READING", data_list, data_package.memory_type
		
		pass

def wait_data_arrive(data_package, stream=None):
	# local_functions
	#################################################################
	def prepare_dest_package_and_dest_devptr(is_new_data, dest_package):
		# ???????????????????
		output_package = dest_package.copy()
		if is_new_data:
			# create new data
			# because we don't have cuda memory allocation for dest_package
			dest_devptr, new_usage = malloc_with_swap_out(output_package.data_bytes)
			output_package.set_usage(new_usage)
			
			#cuda.memset_d8(dest_devptr, 0, output_package.data_bytes)
		else:
			# we already have cuda memory allocation
			# if there are enough halo, we can use exist buffer instead dest_package
			# if there are not enough halo, we have to allocate new buffer
			
			is_new_data_halo = task.dest.data_halo
			exist_data_halo = data_list[u][ss][sp].data_halo
			
			if is_new_data_halo <= exist_data_halo: 
				output_package = data_list[u][ss][sp]
			else:
				output_package = dest_package
			dest_devptr = data_list[u][ss][sp].devptr

		return output_package, dest_devptr
	def check_new_data(data_exist, dest_devptr):
		is_new_data = False
		if not data_exist: # data is not exist
			is_new_data = True
		else:
			# buffer not exist
			if dest_devptr == None: 
				is_new_data = True
		return is_new_data
	
	# initialize variables
	#################################################################
	u, ss, sp = data_package.get_id()
	
	# check recv_list
	flag = True
	if u not in recv_list: flag = False
	elif ss not in recv_list[u]: flag = False
	elif sp not in recv_list[u][ss]: flag = False

	# implementation
	#####################################################################
	if flag:
		# initialize variable
		func_name = 'writing'
		first = recv_list[u][ss][sp][0]
		dt = first['task'].source.data_contents_dtype 
		di = len(first['task'].source.data_shape)
		func_name += '_%dd_%s'%(di, dt)
		
		# prepare copy
		# make dest data
		dest_package = first['task'].dest

		# check data exist
		dest_devptr = None
		data_exist = False
		usage = 0
		if u in data_list:
			if ss in data_list[u]:
				if sp in data_list[u][ss]:
					dest_devptr = data_list[u][ss][sp].devptr
					usage = data_list[u][ss][sp].data_bytes
					data_exist = True

		# data exist check
		is_new_data = check_new_data(data_exist, dest_devptr)
		
		# prepare dest_package and dest_devptr
		dest_package, dest_devptr = prepare_dest_package_and_dest_devptr(is_new_data, dest_package)
		
		dest_data_range = dest_package.data_range
		dest_buffer_range = dest_package.buffer_range
	
		# write from temp data to dest data
		for elem in recv_list[u][ss][sp]:
			# wait data arrive
			#if 'request' in elem: 
				#if elem['request'] != None:MPI.Request.Wait(elem['request'])
				
			# memory copy have release
			if 'mem_release' in elem:
				mem_release(elem['task'].source)
				elem['t_devptr'] = elem['data']
				# we don't need rewrite again
				if data_exist and is_new_data == False:
					# data already existed and keep using same data
					continue
			if elem['data_package'].data_source == 'hdfs':
				t_dp = elem['data_package']
				sp = t_dp.get_split_position()
				stream_position = eval(sp)['z']-1
				hdfs_str  = t_dp.stream_hdfs_file_name
				hdfs_addr = hdfs_str[:hdfs_str.rfind('/')]
				hdfs_path = hdfs_str[hdfs_str.rfind('/')+1:]
	
				import time
				t_before = time.time()
				t_data = load_from_hdfs(t_dp, hdfs_addr, hdfs_path)
				t_diff = time.time() - t_before
				#print "%d th processing data load speed : %.3f MB/s"%(stream_position, t_data.size / 1024.0/1024/t_diff)

				if type(t_data) == numpy.ndarray:
					#t_devptr, usage = malloc_with_swap_out(t_dp.data_bytes,debug_trace=True)
					cuda.memcpy_htod_async(dest_devptr, t_data, stream=stream)
					elem['t_devptr'] = dest_devptr
					elem['usage'] = usage	

					t_dp.memory_type = 'devptr'
					t_dp.devptr = dest_devptr 
					t_dp.data   = dest_devptr

			elif elem['data_package'].data_source == "local":
				t_dp = elem['data_package']
				tu, tss, tsp = t_dp.get_id()
				if tu in data_list:
					if tss in data_list[tu]:
						if tsp in data_list[tu][tss]:
							t_dp.data_local_path = data_list[tu][tss][tsp].data_local_path

				data_path = t_dp.data_local_path 


				#print_blue(t_dp.data_local_path)
				if data_path == '':
					local_dp = first['task'].source
					#print_red(local_dp.info())
					#print_blue(local_dp.data_local_path)
					total_cnt = eval(first['task'].source.get_split_shape())['z']
					dest = 0
					comm.send(rank,					dest=dest,		tag=5)
					comm.send("vivaldi_local",		dest=dest,		tag=5)
					comm.send(total_cnt,			dest=dest,		tag=5)


				else:
					#print_green("MIDDLE")
					center_data = load_data_from_local(t_dp, data_path)
	
	
					if t_dp.data_halo != 0:
						curr_cnt = int(data_path[data_path.rfind('_')+1:])
						
						# upper
						if curr_cnt > 1:
							file_path = "%s_%d"%(data_path[:data_path.rfind('_')], curr_cnt-1)
							for delem in data_loader_list:
								existance = data_check(file_path, delem)
								if existance == True:
									break
	
							upper_data = halo_data_load(t_dp, delem, file_path, "end")
							
							center_data = numpy.concatenate((upper_data, center_data), axis=0)
	
						max_cnt = eval(t_dp.get_split_shape())['z']
	
						#down side
						if curr_cnt < max_cnt-1:
							file_path = "%s_%d"%(data_path[:data_path.rfind('_')], curr_cnt+1)
							for delem in data_loader_list:
								existance = data_check(file_path, delem)
								if existance == True:
									break
	
							lower_data = halo_data_load(t_dp, delem, file_path, "start")
							center_data = numpy.concatenate((center_data, lower_data), axis=0)

	
				#print_blue(dest_package.info())
				#print_red("%s, %s, %s"%(str(center_data.shape),str(center_data.dtype),curr_cnt))
					if type(center_data) == numpy.ndarray:
						dirt_devptr(dest_devptr)
	
						dest_devptr, usage = malloc_with_swap_out(t_dp.data_bytes,debug_trace=True)
					
						cuda.memcpy_htod_async(dest_devptr, center_data, stream=stream)
						elem['t_devptr'] = dest_devptr
						elem['usage'] = usage	

						t_dp.memory_type = 'devptr'
						t_dp.devptr = dest_devptr 
						t_dp.data   = dest_devptr

						u, ss, sp = t_dp.get_id()
						if u in data_list:
							if ss in data_list[u]:
								if sp in data_list[u][ss]:
									data_list[u][ss][sp].devptr = dest_devptr
									#print_blue(socket.gethostname())
									#print_blue(data_list[u][ss][sp].info())
							

				#print_red("################################################################,%s"%t_devptr)

					

			else:
				t_data = elem['data']
				t_dp = elem['data_package']
				work_range = elem['task'].work_range
				t_data_range = t_dp.data_range
				t_buffer_range = t_dp.buffer_range
				t_data_halo = t_dp.data_halo
				t_buffer_halo = t_dp.buffer_halo
				
				if type(t_data) == numpy.ndarray:
					#print t_dp.data_bytes
					t_devptr, usage = malloc_with_swap_out(t_dp.data_bytes,debug_trace=True)
					cuda.memcpy_htod_async(t_devptr, t_data, stream=stream)
					elem['t_devptr'] = t_devptr
					elem['usage'] = usage	
				else:
					t_devptr = t_data
					elem['t_devptr'] = t_devptr
				
				dest_info = data_range_to_cuda_in(dest_data_range, dest_data_range, data_halo=dest_package.data_halo, stream=stream)
				t_info = data_range_to_cuda_in(t_data_range, t_data_range, data_halo=t_data_halo, stream=stream)
				
				
				kernel_write(func_name, dest_devptr, dest_info, t_devptr, t_info, work_range, stream=stream)

				# update dirty flag for t_devptr (old pointer)
				dirt_devptr(t_devptr)

			
			target = (u,ss,sp)
			if target not in gpu_list:
				gpu_list.append((u,ss,sp))

		#stream.synchronize()
		for elem in recv_list[u][ss][sp]:
			t_data = elem['data']
			t_dp = elem['data_package']
			work_range = elem['task'].work_range
			t_data_range = t_dp.data_range
			t_devptr = elem['t_devptr']
			usage = elem['usage']

			if 'temp_data' in elem:
				data_pool_append(data_pool, t_devptr, usage)


		dest_package.memory_type = 'devptr'
		dest_package.data_dtype = 'cuda_memory'
		dest_package.devptr = dest_devptr


		if not data_exist:
			if u not in data_list: data_list[u] = {}
			if ss not in data_list[u]: data_list[u][ss] = {}
		data_list[u][ss][sp] = dest_package
		

		del recv_list[u][ss][sp]
		if recv_list[u][ss] == {}: del recv_list[u][ss]
		if recv_list[u] == {}: del recv_list[u]


	else:
		# data already in here
		pass

def send(data, data_package, dest=None, gpu_direct=True):
	global s_requests
	tag = 52
	dp = data_package
	# send data_package
	send_data_package(dp, dest=dest, tag=tag)

	bytes = dp.data_bytes
	memory_type = dp.memory_type
	
	if log_type in ['time','all']: st = time.time()

	flag = False
	request = None
	if memory_type == 'devptr': # data in the GPU
		if False: # want to use GPU direct
			devptr = data
			buf = MPI.make_buffer(devptr.__int__(), bytes)
			ctx.synchronize()
			request = comm.Isend([buf, MPI.BYTE], dest=dest, tag=57)
			if VIVALDI_BLOCKING: MPI.Request.Wait(request)
			s_requests.append((request, buf, devptr))
			flag = True
		else:# not want to use GPU direct
			#print_red(memory_type)
		
			# copy to CPU
			shape = dp.data_memory_shape
			dtype = dp.data_contents_memory_dtype
			buf = numpy.empty(shape, dtype=dtype)
			cuda.memcpy_dtoh_async(buf, data, stream=stream_list[1])

			#try:
				##print bcolors.OKBLUE, dest, rank, bcolors.ENDC
				#comm.send(buf, dest=dest, tag=57)
				#import Image
				#Image.fromarray(buf).save("/home/freyja/result/tmp__%s.png"%(socket.gethostname()))
			#except:
				#print "FAILED"
				#pass

			comm.send(buf, dest=dest, tag=57)

			#request = comm.Isend(buf, dest=dest, tag=57)
			#if VIVALDI_BLOCKING: MPI.Request.Wait(request)
			#s_requests.append((request, buf, None))
			
	else: # data in the CPU
		# want to use GPU direct, not exist case
		# not want to use GPU direct
		if dp.data_dtype == numpy.ndarray:
			comm.send(data, dest=dest, tag=57)
			#request = comm.Isend(data, dest=dest, tag=57)
			#if VIVALDI_BLOCKING: MPI.Request.Wait(request)
			#s_requests.append((request, data, None))
			
	if log_type in ['time','all']:
		u = dp.unique_id
		bytes = dp.data_bytes
		t = MPI.Wtime()-st
		ms = 1000*t
		bw = bytes/GIGA/t
	
		if flag:
			log("rank%d, \"%s\", u=%d, from rank%d to rank%d GPU direct send, Bytes: %dMB, time: %.3f ms, speed: %.3f GByte/sec"%(rank, name, u, rank, dest, bytes/MEGA, ms, bw),'time', log_type)
		else:
			log("rank%d, \"%s\", u=%d, from rank%d to rank%d MPI data transfer, Bytes: %dMB, time: %.3f ms, speed: %.3f GByte/sec"%(rank, name, u, rank, dest, bytes/MEGA, ms, bw),'time', log_type)
	
	return request
def recv():
	# DEBUG flag
	################################################
	RECV_CHECK = False
	
	# Implementation
	################################################
	data_package = comm.recv(source=source,	tag=52)
	dp = data_package
	memory_type = dp.memory_type


	# if data exist on GPU and not hdfs
	if memory_type == 'devptr' and not dp.stream:
		bytes = dp.data_bytes
		devptr, usage = malloc_with_swap_out(bytes)
		

		# Non-blocking recv
		#ds = dp.data_shape
		#dt = dp.data_contents_memory_dtype
		#buf = MPI.make_buffer(devptr.__int__(), bytes)
		#buf = numpy.empty(ds, dtype=dt)
		#request = comm.Irecv([buf, MPI.BYTE], source=source, tag=57)

		#request = comm.irecv(buf, dest=source, tag=57)
		#MPI.Request.Wait(request)

		# blocking recv
		buf = comm.recv(source=source, tag=57)

		#cuda.memcpy_htod(devptr, buf)

		

		return devptr, data_package, None, buf

	else:
		bytes = dp.data_bytes
		data_dtype = dp.data_dtype
		if data_dtype == numpy.ndarray:
			data_memory_shape = dp.data_memory_shape
			dtype = dp.data_contents_memory_dtype
			data = numpy.empty(data_memory_shape, dtype=dtype)
			if bytes > 2 * 1024 ** 3 and not dp.stream:
				blocks = bytes / (2*1024**3) + 1
				total_len = len(data)
				transfer_unit = total_len/ blocks
				begin = 0
				for elem in range(blocks):
					#request = comm.Irecv(data, source=source, tag=57+elem)
					request = None
					data[begin:begin+transfer_unit] = comm.recv(source=source, tag=57+elem)
					begin += transfer_unit
			else:
				#request = comm.Irecv(data, source=source, tag=57)
				request = None
				data = comm.recv(source=source, tag=57)
			#open("/home/freyja/result/input%s.raw"%(socket.gethostname()), "wb").write(data)
			#print_red("from %d  ,"%source+str(data.shape))
			
			if RECV_CHECK: # recv check
				MPI.Request.Wait(request)
				print "RECV CHECK", data

			if VIVALDI_BLOCKING: MPI.Request.Wait(request)

		return data, data_package, request, None

	return None,None,None,None


def save_as_local(data_package, dest_devptr):
	dp = data_package
	devptr = dest_devptr
	local_data_shape = dp.data_shape
	local_data_dtype = Vivaldi_dtype_to_python_dtype(dp.data_contents_dtype)
			
	buf = numpy.empty(local_data_shape, dtype=local_data_dtype)

	#print_green(dp.info())
	# memcpy from Device to Host
	cuda.memcpy_dtoh(buf, devptr)

	path = "/scratch/%s/VIVALDI/"%(getpass.getuser())
	file_name = "VIVALDI%s_%d"%(dp.get_unique_id(),eval(dp.get_split_position())['z']-1)
	f = open(path+file_name, "wb")

	f.write(buf.tostring())

	dp.data_local_path = path + file_name



def notice_to_main(data_package, dest_devptr):
	# SEND TO MAIN (done message)
	dp = data_package
	devptr = dest_devptr
	local_data_shape = dp.data_shape
	local_data_dtype = Vivaldi_dtype_to_python_dtype(dp.data_contents_dtype)
			
	buf = numpy.empty(local_data_shape, dtype=local_data_dtype)

	#print_green(dp.info())
	# memcpy from Device to Host
	cuda.memcpy_dtoh(buf, devptr)

	path = "/scratch/%s/VIVALDI/"%(getpass.getuser())
	file_name = "VIVALDI%s_%d"%(dp.get_unique_id(),eval(dp.get_split_position())['z']-1)
	f = open(path+file_name, "wb")

	f.write(buf.tostring())

	dp.data_local_path = path + file_name


	total_count = eval(dp.get_split_shape())['z']

	tmp_data   = dp.data
	tmp_devptr = dp.devptr

	dp.data   = None
	dp.devptr = None

	dest = 0
	comm.send(rank,					dest=dest,		tag=5)
	comm.send("vivaldi_local",		dest=dest,		tag=5)
	comm.send(total_count,  		dest=dest,		tag=5)
	comm.send(dp,					dest=dest,		tag=52)


	dp.data   = tmp_data
	dp.devptr = tmp_devptr

def memcpy_p2p_send(task, dest):
	# initialize variables
	ts = task.source
	u, ss, sp = ts.get_id()
	
	wr = task.work_range
	
	dest_package = task.dest
	du, dss, dsp = dest_package.get_id()

	func_name = 'writing'
	dt = ts.data_contents_dtype 
	di = len(ts.data_shape)
	func_name += '_%dd_%s'%(di, dt)

	if du not in dirty_flag: dirty_flag[du] = {}
	if dss not in dirty_flag[du]: dirty_flag[du][dss] = {}
	if dsp not in dirty_flag[du][dss]: dirty_flag[du][dss][dsp] = True

	# wait source is created
	target = (u,ss,sp)
	if target in valid_list:
		Event_dict[target].synchronize()
	else:
		wait_data_arrive(task.source)
		
	# check copy to same rank or somewhere else
	sr = (task.execid == rank)
	#print "p2p_send", task.source.data_range, dest_package.data_range, wr, rank, dest, sr
	#if dest_package.data_source == 'local':
		#if dest != 0:
			##print_blue(task.source.info())
			##task.source.data_halo = 1
			#notice(task.source)
		#pass
	if sr:
		# source is not necessary any more
		if du not in recv_list: recv_list[du] = {}
		if dss not in recv_list[du]: recv_list[du][dss] = {}
		if dsp not in recv_list[du][dss]: recv_list[du][dss][dsp] = []

		if u not in data_list:
			return

		#print "DT", data_list[u][ss][sp].info()
		recv_list[du][dss][dsp].append({'task':task, 'data':data_list[u][ss][sp].devptr, 'data_package':data_list[u][ss][sp],'mem_release':True,'usage':data_list[u][ss][sp].usage})

		if VIVALDI_BLOCKING:
			wait_data_arrive(task.dest)
		notice(task.dest)
	else:
		# different machine
		# check data will be cuted or not
		if u not in data_list:
			return
		source_package = data_list[u][ss][sp]

		bytes = 0
		data_halo = 0
		
		# make data package
		dp = data_list[u][ss][sp].copy()
		dp.unique_id = u
		dp.split_shape = ss
		dp.split_position = sp
		dp.data_halo = data_halo
		dp.set_data_range(wr)
		
		# wait real data arrive
		target = (u,ss,sp)
		if target in Event_dict:
			Event_dict[target].synchronize()
		
		if target not in valid_list:
			wait_data_arrive(dp)
		
		# prepare send buffer
		cut = False
		if wr != source_package.data_range: cut = True
		# prepare data
		if cut:
			dp.set_buffer_range(0)
			
			bcms = source_package.data_contents_memory_shape
			bcmd = source_package.data_contents_memory_dtype
			bcmb = get_bytes(bcmd)

			bytes = make_bytes(wr, bcms, bcmb)
			dest_devptr, usage = malloc_with_swap_out(bytes)
			start = time.time()
			
			dest_info = data_range_to_cuda_in(wr, wr, wr, stream=stream_list[1])
			source_info = data_range_to_cuda_in(source_package.data_range, source_package.full_data_range, source_package.buffer_range, data_halo=source_package.data_halo, buffer_halo=source_package.buffer_halo, stream=stream_list[1])
			
			kernel_write(func_name, dest_devptr, dest_info, source_package.devptr, source_info, wr, stream=stream_list[1])
			
		
			start = time.time()
			data_halo = 0
			temp_data = True
			
		else:
			dest_devptr = source_package.devptr
			bytes = source_package.data_bytes
			data_halo = source_package.data_halo
			usage = source_package.usage
			temp_data = False

		# now dest_devptr has value
	
		#print "TEST %s"%(dp.data_source)
		if dp.data_source != 'local':
		#if False:
			# say dest to prepare to recv this data
			comm.isend(rank,				dest=dest,	tag=5)
			comm.isend("memcpy_p2p_recv",	dest=dest,	tag=5)
			comm.isend(task,				dest=dest,	tag=57)
			comm.isend(data_halo,			dest=dest,	tag=57)
		
			# send data
			#print_devptr(dest_devptr, dp)
			#print_green("sent from %s to %d"%(socket.gethostname(), dest))
		
			data = dest_devptr
			stream_list[1].synchronize()

			request = send(data, dp, dest=dest, gpu_direct=GPUDIRECT)

		else:
			#comm.isend(rank,				dest=0,	tag=25)
			comm.isend(rank,				dest=dest,	tag=5)
			comm.isend("memcpy_p2p_recv",	dest=dest,	tag=5)
			comm.isend(task,				dest=dest,	tag=57)
			comm.isend(0,			dest=dest,	tag=57)

			comm.send(task.dest,			dest=dest,	tag=52)
			comm.send(0,			dest=dest,	tag=57)
			#print_red(task.source.data_halo)
			#notice(task.dest)
			#print_blue(task.dest.info())
			pass

		#if temp_data:
			#data_pool_append(data_pool, dest_devptr, usage, request=request)

def memcpy_p2p_recv(task, data_halo):
	# initialize variables
	dest_package = task.dest
	du, dss, dsp = dest_package.get_id()
	
	func_name = 'writing'
	dt = task.source.data_contents_dtype 
	di = len(task.source.data_shape)
	func_name += '_%dd_%s'%(di, dt)

	data, data_package, request, buf = recv()
	dp = data_package


	#FREYJA STREAMING
	#if dest_package.stream:
		#stream_position = eval(dsp)['z']-1
		#dest_package.file_name += dest_package.stream_file_list[stream_position]

		#fp = open(dest_package.file_name,'rb')

       
		#print "START TO LOAD DATA from %d th sequence"%(stream_position)
		#data = load_data(dest_package, dest_package.data_range, fp=fp)
		#print "END TO LOAD DATA from %d th sequence"%(stream_position)
		#dest_package.data = data


	# data exist check
	if du not in data_list: 
		data_exist = False
		data_list[du] = {}
	if dss not in data_list[du]:
		data_exist = False
		data_list[du][dss] = {}
	if dsp not in data_list[du][dss]:
		data_exist = False
		data_list[du][dss][dsp] = dest_package
		

	# data already exist
	if du not in recv_list: recv_list[du] = {}
	if dss not in recv_list[du]: recv_list[du][dss] = {}
	if dsp not in recv_list[du][dss]: recv_list[du][dss][dsp] = []
	recv_list[du][dss][dsp].append({'request':request,'task':task, 'data':data, 'data_package':dest_package, 'buf': buf, 'temp_data':True,'usage':dp.data_bytes})

	#print_red(dest_package.info())
	notice(dest_package)
	return 

def mem_release(data_package):
	comm.isend(rank,                dest=1,    tag=5)
	comm.isend("release",           dest=1,    tag=5)
	send_data_package(data_package, dest=1,    tag=57)
	
# Communication with scheduler
def send_data_package(data_package, dest=None, tag=None):
	dp = data_package
	t_data, t_devptr = dp.data, dp.devptr
	dp.data, dp.devptr = None, None
	comm.isend(dp, dest=dest, tag=tag)
	dp.data, dp.devptr = t_data, t_devptr
	t_data,t_devptr = None, None

def idle():
	rank = comm.Get_rank()
	comm.isend(rank,                dest=1,    tag=5)
	comm.isend("idle",              dest=1,    tag=5)

def notice(data_package, target=rank):
	rank = comm.Get_rank()
	comm.isend(rank,                dest=1,    tag=5)
	comm.isend("notice",            dest=1,    tag=5)
	comm.isend(target,            	dest=1,    tag=5)
	send_data_package(data_package, dest=1,    tag=51)
	#print_blue(data_package.info())
	#print_green(target)
	#traceback.print_stack()


def data_check(file_path, dest):
	rank = comm.Get_rank()
	comm.isend(rank,                dest=dest,    tag=5)
	comm.isend("data_check",        dest=dest,    tag=5)
	comm.isend(file_path,			dest=dest,    tag=55)
	#send_data_package(data_package, dest=dest,    tag=52)
	#print "SENT"

	reply = comm.recv(source=dest, tag=11)
	#print_blue(data_package.info())
	return reply

def halo_data_load(data_package, dest, file_path, location):
	rank = comm.Get_rank()
	comm.isend(rank,                dest=dest,	tag=5)
	comm.isend("data_load",        	dest=dest,	tag=5)
	send_data_package(data_package, dest=dest,	tag=52)
	comm.isend(file_path,        	dest=dest,	tag=55)
	comm.isend(location,        	dest=dest,	tag=55)

	data = comm.recv(source=dest,	tag=57)

	return data
	
	
	
# local functions
def get_function_dict(x):
	function_code_dict = {}
	def get_function_name_list(code=''):
		def get_head(code='', st=0):
			idx = code.find('def ', st)
			if idx == -1: return None, -1
			idx2 = code.find(':', idx+1)
			head = code[idx+4:idx2]
			head = head.strip()
			return head, idx2+1	
		def get_function_name(head=''):
			idx = head.find('(')
			return head[:idx]
			
		function_name_list = []
		st = 0
		while True:
			head, st = get_head(code, st)
			if head == None: break
			function_name = get_function_name(head)
			function_name_list.append(function_name)
		return function_name_list	
	def get_code(function_name='', code=''):
		if function_name == '':
			print "No function name found"
			assert(False)
		# initialization
		###################################################
		st = 'def '+function_name+'('
		output = ''
		s_idx = code.find(st)
		
		if s_idx == -1:
			print "Error"
			print "Cannot find the function"
			print "Function want to find:", function_name
			assert(False)
		n = len(code)
		
		
		# implementation
		###################################################
		
		# there are n case function finish
		# ex1) code end
		# def main()
		# 	...
		#
		# ex2) another function 
		# def main()
		# 	...
		# def func():
		#
		# ex3) indent
		# def main()
		# 	...
		# print 
		
		# there are n case main not finish
		# ex1) main
		# def main():
		#     ArithmeticError
		#
		#     BaseException
		
		def get_indent(line): 
			s_line = line.strip()
			i_idx = line.find(s_line)
			indent = line[:i_idx]

			return indent
			
		line = ''
		cnt = 1
		i = s_idx
		while i < n:
			w = code[i]
			
			line += w
			if w == '\n':
				indent = get_indent(line)
				if indent == '' and line.strip().startswith('def'):
					# ex2 and ex3
					if cnt == 0:break
					cnt -= 1
				
				output += line
				line = ''
			i += 1
		
		return output
		
	function_name_list = get_function_name_list(x)
	for function_name in function_name_list:
		function_code = get_code(function_name=function_name, code=x)
		function_code_dict[function_name] = function_code
	return function_code_dict
def save_data(data, data_package):
	# if data is numpy.ndarray, copy to GPU and save only devptr
	dp = data_package

	data_name = dp.data_name
	data_range = dp.data_range
	shape = dp.data_shape
	u, ss, sp = dp.get_id()
	
	#log("rank%d, \"%s\", u=%d, saved"%(rank, data_name, u),'general',log_type)
	buf_dtype = type(data)

	if buf_dtype == numpy.ndarray:
		dp.memory_type = 'devptr'
		dp.data_dtype = 'cuda_memory'
		dp.devptr = to_device_with_time(data, data_name, u)
	elif buf_dtype == cuda.DeviceAllocation:
		dp.memory_type = 'devptr'
		dp.data_dtype = 'cuda_memory'
		dp.devptr = data
	elif buf_dtype == 'cuda_memory':
		pass
	else:
		assert(False)

	if dp.devptr == None:
		assert(False)
	target = (u,ss,sp)
	if target not in gpu_list:
		gpu_list.append(target)

	# save image
	if log_type in ['image', 'all']:
		if len(dp.data_shape) == 2 and dp.memory_type == 'devptr':
			image_data_shape = dp.data_memory_shape
			md = dp.data_contents_memory_dtype
			a = numpy.empty(image_data_shape,dtype=md)
#			cuda.memcpy_dtoh_async(a, dp.devptr, stream=stream[1])
			cuda.memcpy_dtoh(a, dp.devptr)
			ctx.synchronize()

			buf = a
			extension = 'png'
			dtype = dp.data_contents_dtype
			chan = dp.data_contents_memory_shape
			buf = buf.astype(numpy.uint8)
				
			if chan == [1]: img = Image.fromarray(buf, 'L')
			elif chan == [3]: img = Image.fromarray(buf, 'RGB')
			elif chan == [4]: img = Image.fromarray(buf, 'RGBA')
					
			e = os.system("mkdir -p result")
			img.save('./result/%s%s%s.png'%(u,ss,sp), format=extension)


	u = dp.get_unique_id()
	if u not in data_list: data_list[u] = {}
	if ss not in data_list[u]:data_list[u][ss] = {}
	data_list[u][ss][sp] = dp
def data_finder(u, ss, sp, gpu_direct=True):
	data_package = data_list[u][ss][sp]
	dp = data_package.copy()

	memory_type = dp.memory_type
	if memory_type == 'devptr':
		if gpu_direct:
			devptr = data_list[u][ss][sp].devptr
			return devptr, dp
		else:
			devptr = data_list[u][ss][sp].devptr
			shape = dp.data_memory_shape
			bcmd = dp.data_contents_memory_dtype
			if log_type in ['time','all']: st = time.time()

			buf = numpy.empty((shape), dtype=bcmd)
			cuda.memcpy_dtoh_async(buf, devptr, stream=stream[1])
#			buf = cuda.from_device(devptr, shape, bcmd)
			if log_type in ['time','all']:
				u = dp.unique_id
				bytes = dp.data_bytes
				t = MPI.Wtime()-st
				ms = 1000*t
				bw = bytes/GIGA/t
				log("rank%d, \"%s\", u=%d, GPU%d data transfer from GPU memory to CPU memory, Bytes: %dMB, time: %.3f ms, speed: %.3f GByte/sec"%(rank, name, u, device_number, bytes/MEGA, ms, bw),'time', log_type)
			
			dp.memory_type = 'memory'
			dp.data_dtype = type(buf)
			return buf, dp
	else:
		data = data_list[u][ss][sp].data
		return data, dp
	return None, None
def to_device_with_time(data, data_name, u):
	st = time.time()

	devptr, usage = malloc_with_swap_out(data.nbytes)
	cuda.memcpy_htod(devptr, data)
		
	t = time.time()-st
	ms = 1000*t
	bw = data.nbytes/GIGA/t
	bytes = data.nbytes
	#log("rank%d, \"%s\", u=%d, to GPU%d memory transfer, Bytes: %0.2fMB, time: %.3f ms, speed: %.3f GByte/sec"%(rank, data_name, u, device_number, bytes/MEGA, ms, bw), 'time', log_type)
	return devptr
# CUDA support functions
def print_devptr(devptr, data_package):
	try:
		data_range = data_package.data_range
		dtype = data_package.data_contents_memory_dtype
		data_memory_shape = data_package.data_memory_shape
		data = numpy.empty(data_memory_shape, dtype=dtype)
		cuda.memcpy_dtoh(data, devptr)
		
		print "PRINT DEVPTR", data, data.info(), data.min(), data.max()
		
	except:
		print data_package.info()
global KD
KD = []
def kernel_write(function_name, dest_devptr, dest_info, source_devptr, source_info, work_range, stream=None):
	global KD

	# initialize variables
	global tb_cnt
	tb_cnt = 0

	# dest
	cuda_args = [dest_devptr]
	cuda_args += [dest_info]

	# source
	cuda_args += [source_devptr]
	cuda_args += [source_info]

	# work_range
	cuda_args += make_cuda_list(work_range)

	mod = source_module_dict['default']
	
	func = mod.get_function(function_name)
	
	# set work range
	block, grid = range_to_block_grid(work_range)
	#block = (1024,1,1)
	#grid = (numpy.int32(786432/4/1024)+1,1)
	
	if log_type in ['time', 'all']:
		st = time.time()

#	print "BL", block, grid
	func(*cuda_args, block=block, grid=grid, stream=stream)
	ctx.synchronize()
	
	KD.append((dest_info, source_info))

	#for debuging


	if log_type in ['time', 'all']:
		bytes = make_bytes(work_range,3)
		t = MPI.Wtime()-st
		ms = 1000*t
		bw = bytes/GIGA/t
		log("rank%d, GPU%d, , kernel write time, Bytes: %dMB, time: %.3f ms, speed: %.3f GByte/sec "%(rank, device_number, bytes/MEGA, ms, bw),'time', log_type)
	

def CustomSourceModule(kernel_code):
	a = None
	try:
		a = SourceModule(kernel_code, no_extern_c = True, options = ["-use_fast_math", "-O3", "-w"])
	except:
		import sys, traceback
		print "Python Trace back"
		print "======================"
		exc_type, exc_value, exc_traceback = sys.exc_info()
		a = ''.join(traceback.format_exception(exc_type, exc_value, exc_traceback))
		idx = a.find('kernel.cu(')
		aa = a[idx:]
		print aa

		eidx = aa.find(')')
		num = aa[10:eidx]
		line = int(num)
		print "Error code line:"
		print kernel_code.split("\n")[line-1:line][0]
		print "======================"

		exit()

	return a

# CUDA compile functions
def compile_for_GPU(function_package, kernel_function_name='default'):
	kernel_code = ''
	if kernel_function_name == 'default':
		kernel_code = attachment
		source_module_dict[kernel_function_name] = CustomSourceModule(kernel_code)
	else:
		fp = function_package
		
		from vivaldi_translator import translate_to_CUDA
		function_name = fp.function_name

		Vivaldi_code = function_code_dict[function_name]
		dtype_dict = {}
		if fp.stream:
			dtype_dict['rb'] = fp.output.data_contents_dtype +'_volume'

		
		function_code = translate_to_CUDA(Vivaldi_code=Vivaldi_code, function_name=function_name, function_arguments=fp.function_args, dtype_dict=dtype_dict)
		
		kernel_code = attachment + 'extern "C"{\n'
		kernel_code += function_code
		kernel_code += '\n}'

		if True: # print for debugging
			f = open('asdf.cu','w')
			f.write(kernel_code)
			f.close()
			f = open('cuda_code.cu','w')
			f.write(kernel_code)
			f.close()


		#print function_code
		args = [kernel_code]
		source_module_dict[kernel_function_name] = CustomSourceModule(kernel_code)

		temp,_ = source_module_dict[kernel_function_name].get_global('DEVICE_NUMBER')
		cuda.memcpy_htod(temp, numpy.array(device_number,dtype=numpy.int32))
		
		func_dict[kernel_function_name] = source_module_dict[kernel_function_name].get_function(kernel_function_name)
		
		create_helper_textures(source_module_dict[kernel_function_name])
dummy = []
def create_helper_textures(mod):
	"create lookup textures for cubic interpolation and random generation"

	def hg(a):
		a2 = a * a
		a3 = a2 * a
		w0 = (-a3 + 3*a2 - 3*a + 1) / 6
		w1 = (3*a3 - 6*a2 + 4) / 6
		w2 = (-3*a3 + 3*a2 + 3*a + 1) / 6
		w3 = a3 / 6
		g = w2 + w3
		h0 = 1 - w1 / (w0 + w1) + a
		h1 = 1 + w3 / (w2 + w3) - a
		return h0, h1, g, 0

	def dhg(a):
		a2 = a * a
		w0 = (-a2 + 2*a - 1) / 2
		w1 = (3*a2 - 4*a) / 2
		w2 = (-3*a2 + 2*a + 1) / 2
		w3 = a2 / 2
		g = w2 + w3
		h0 = 1 - w1 / (w0 + w1) + a
		h1 = 1 + w3 / (w2 + w3) - a
		return h0, h1, g, 0

	tmp = numpy.zeros((256, 4), dtype=numpy.float32)
	for i, x in enumerate(numpy.linspace(0, 1, 256)):
		tmp[i, :] = numpy.array(hg(x))

	tmp = numpy.reshape(tmp, (1, 256, 4))

	hg_texture = create_2d_rgba_texture(tmp, mod, 'hgTexture')
	tmp = numpy.zeros((256, 4), dtype=numpy.float32)
	for i, x in enumerate(numpy.linspace(0, 1, 256)):
		tmp[i, :] = numpy.array(dhg(x))
	tmp = numpy.reshape(tmp, (1, 256, 4))
	dhg_texture = create_2d_rgba_texture(tmp, mod, 'dhgTexture')

	tmp = numpy.zeros((256, 256), dtype=numpy.uint32) # uint32(rand(256, 256) * (2 << 30))
	random_texture = create_2d_texture(tmp, mod, 'randomTexture', True)
	update_random_texture(random_texture)

	# to prevent GC from destroying the textures
	global dummy
	dummy.append((hg_texture, dhg_texture))
def update_random_texture(random_texture):
	tmp = numpy.uint32(rand(256, 256) * (2 << 30))
	update_2d_texture(random_texture, tmp)

# CUDA malloc function
def cpu_mem_check():
	f = open('/proc/meminfo','r')
	s = f.read()
	f.close()

	idx = s.find('MemFree')
	s = s[idx:]
	idx1 = s.find(':')
	idx2 = s.find('kB')
	s = s[idx1+1:idx2]

	MemFree = s.strip()
	MemFree = int(MemFree)*1024
	return MemFree
def swap_out_to_CPU(elem):
	# prepare variables
	return_falg = True
	u, ss, sp = elem
	dp = data_list[u][ss][sp]
	bytes = dp.data_bytes

	# now we will swap out, this data to CPU
	# so first we should check CPU has enough free memory

	MemFree = cpu_mem_check()

	if log_type in ['memory']:
		fm,tm = cuda.mem_get_info()
		log_str = "CPU MEM CEHCK Before swap out: %s Free, %s Maximum, %s Want to use"%(print_bytes(MemFree),'-',print_bytes(bytes))
		log(log_str,'memory',log_type)


	if bytes > MemFree:
		# not enough memory for swap out to CPU
		return False
	
	# we have enough memory so we can swap out
	# if other process not malloc during this swap out oeprataion

	try:
		buf = numpy.empty((dp.data_memory_shape), dtype= dp.data_contents_memory_dtype)
	except:
		# we failed memory allocation in the CPU
		return False

	# do the swap out
	#cuda.memcpy_dtoh_async(buf, dp.devptr, stream=stream[1])
	cuda.memcpy_dtoh(buf, dp.devptr)
	ctx.synchronize()

	dp.devptr.free()
	dp.devptr = None
	dp.data = buf
	dp.data_dtype = numpy.ndarray
	dp.memory_type = 'memory'


	gpu_list.remove(elem)
	cpu_list.append(elem)

	if log_type in ['memory']:
		fm,tm = cuda.mem_get_info()
		log_str = "GPU MEM CEHCK After swap out: %s Free, %s Maximum, %s Want to use"%(print_bytes(fm),print_bytes(tm),print_bytes(bytes))
		
		log(log_str,'memory',log_type)


	return True
def swap_out_to_hard_disk(elem):
	# prepare variables
	return_falg = True
	u, ss, sp = elem
	dp = data_list[u][ss][sp]
	bytes = dp.data_bytes

	# now we will swap out, this CPU to hard disk
	# so first we should check hard disk has enough free memory
	file_name = '%d_temp'%(rank)
	os.system('df . > %s'%(file_name))

	f = open(file_name)
	s = f.read()
	f.close()

	ss = s.split()

	# get available byte
	avail = int(ss[10])

	if log_type in ['memory']:
		fm,tm = cuda.mem_get_info()
		log_str = "HARD disk MEM CEHCK Before swap out: %s Free, %s Maximum, %s Want to use"%(print_bytes(avail),'-',print_bytes(bytes))
		log(log_str,'memory',log_type)

	if bytes > avail:
		# we failed make swap file in hard disk
		return False

	# now we have enough hard disk to make swap file
	# temp file name, "temp_data, rank, u, ss, sp"
	file_name = 'temp_data, %s, %s, %s, %s'%(rank, u, ss, sp)
	f = open(file_name,'wb')
	f.write(dp.data)
	f.close()

	dp.data = None
	dp.hard_disk = file_name
	dp.memory_type = 'hard_disk'

	cpu_list.remove(elem)
	hard_list.append(elem)
	
	if log_type in ['memory']:
		fm,tm = cuda.mem_get_info()
		log_str = "CPU MEM CEHCK After swap out: %s Free, %s Maximum, %s Want to use"%(print_bytes(fm),print_bytes(tm),print_bytes(bytes))
		log(log_str,'memory',log_type)

	return True



def gpu_mem_check_and_free():
	smallest = float('inf')
	
	#print "From %s, %s"%(socket.gethostname(),str(devptr_and_timestamp))
	#print u, ss, sp
	for _elem in devptr_and_timestamp:
		_updated   = _elem['updated']
		if _updated == False:
			_timestamp = _elem['timestamp']
			if _timestamp < smallest:
				_devptr    = _elem['devptr']
				_devptr.free()

				devptr_and_timestamp.remove(_elem)
				return True

	return False

def update_devptr_timestamp(devptr):
	for _elem in devptr_and_timestamp:
		if devptr == _elem['devptr']:
			_elem['timestamp'] = time.time()
			_elem['updated'] = True
			

def dirt_devptr(devptr):
	for _elem in devptr_and_timestamp:
		if devptr == _elem['devptr']:
			_elem['updated'] = False

	


def mem_check_and_malloc(bytes):
	fm,tm = cuda.mem_get_info()

	if log_type in ['memory']:
		log_str = "RANK %d, GPU MEM CEHCK before malloc: %s Free, %s Maximum, %s Want to use"%(rank, print_bytes(fm),print_bytes(tm),print_bytes(bytes))
		log(log_str,'memory',log_type)
		
	# we have enough memory


	while fm < bytes:
		# we don't have enough memory, free data fool
		mem_reset_flag = gpu_mem_check_and_free()
		
		if mem_reset_flag == False:
			print "NOT ENOUGH MEMORY"
			assert(mem_reset_flag)
	
		fm,tm = cuda.mem_get_info()

	if fm >= bytes:
		# we have enough memory, just malloc
		afm,tm = cuda.mem_get_info()	
		#print bcolors.BOLD, traceback.print_stack(), bcolors.ENDC
		#print_purple("allocated bytes :  %d"%bytes, socket.gethostname())
		devptr = cuda.mem_alloc(bytes)

		devptr_and_timestamp.append({'devptr':devptr, 'timestamp':time.time(), 'updated':True})

		bfm,tm = cuda.mem_get_info()
		#print_purple( data_pool, socket.gethostname())
		if log_type in ['memory']:
			fm,tm = cuda.mem_get_info()
			log_str = "RANK %d, GPU MALLOC AFTER: %s Free, %s Maximum, %s Want to use"%(rank, print_bytes(fm),print_bytes(tm),print_bytes(bytes))
			log(log_str, 'memory', log_type)
		return True, devptr

	# we don't have enough memory
	return False, None
def data_pool_append(data_pool, devptr, usage, request=None):
	data_pool.append({'devptr':devptr, 'usage':usage, 'request':request})
def find_reusable(bytes):
	min = -1
	selected = None

	for elem in data_pool:
		# usage check
		usage = elem['usage']
		if usage < bytes: continue
		if min < usage and min != -1: continue
		if bytes*2 < usage: continue
		
		# request check
		request = elem['request']
		if request != None:
			status = MPI.Status()
			flag = request.Test(status)
			if flag == False: continue
			
		min = usage
		selected = elem

	if selected != None:

		if min < bytes:
			print min, bytes
			assert(False)

		data_pool.remove(selected)
		devptr = selected['devptr']
	#	print "RECYCLE", min, "for", bytes, "DEVPTR", devptr, len(data_pool), rank

		return True, devptr, min # flag, devptr, data size

	return False, None, 0
def malloc_with_swap_out(bytes, arg_lock=[], debug_trace=False):
	#if debug_trace == False:
		#print bytes
		#assert(False)

	# initialize data
	devptr = False

	# check is there reusable data, because malloc and free is expensive operator
	flag, devptr, usage = find_reusable(bytes)

	if flag:
		# reuse empty data
		return devptr, usage

	flag, devptr = mem_check_and_malloc(bytes)
	if flag:
		# we have enough memory
	#	print "MALLOC", bytes, rank
#		print gpu_list
		return devptr, bytes
	else:
		print data_pool

	print "We don't have enough memory"
	assert(False)
	# we don't have enough memory, we will try swap out one by one
	for elem in gpu_list:	
		# check we can free or not 
		if elem in arg_lock:
			# this argus should not be swap out, pass 
			continue

	
		while True:
			# swap out to CPU memory, first 
			# if impossible, swap out to hard dsik
			flag1 = swap_out_to_CPU(elem)
			if flag1:
				# swap out succeed
				flag, devptr = mem_check_and_malloc(bytes)
				if flag:
					# malloc succeed
					return devptr

			if flag == False:
				# swap out to CPU failed
				# we need swap out to hard disk
				for elem2 in cpu_list:
					flag1 = swap_out_to_hard_disk(elem2)
					flag2 = swap_out_to_CPU_memory(elem)

					if flag2:
						# swap out to CPU succed
						flag, devptr = mem_check_and_malloc(bytes)
						if flag:
							# malloc succeed
							return devptr
				
				# swap out to hard disk failed
				print "------------------------------------"
				print "VIVALDI ERROR: SWAP OUT to hard dsik failed, may be you don't have enough hard disk sotrage"
				print "WE TRIED WRITE %s to HARD DSIK"%(print_bytes(bytes))
				print "------------------------------------"
				assert(False)

	print "------------------------------------"
	print "VIVALDI ERROR: we shouldn't not be here something wrong"
	print "swap out failed"
	print "------------------------------------"
	assert(False)
	return devptr

# CUDA execute function
global a
a = numpy.zeros((26),dtype=numpy.int32)
def data_range_to_cuda_in(data_range, full_data_range, buffer_range=None, data_halo=0, buffer_halo=0, stream=None):
#	global a
	i = 0 
	a = numpy.empty((26), dtype=numpy.int32)
	for axis in AXIS:
		if axis in data_range:
			a[i] = data_range[axis][0]
			a[i+4] = data_range[axis][1]
			a[i+8] = full_data_range[axis][0]
			a[i+12] = full_data_range[axis][1]
			# i'm thinking about not use buffer range any more
			# but we can't change GPU immediately
			#a[i+16] = buffer_range[axis][0] 
			#a[i+16] = buffer_range[axis][0]
			a[i+16] = data_range[axis][0]
			a[i+20] = data_range[axis][1]
		else:
			a[i] = numpy.int32(0)
			a[i+4] = numpy.int32(0)
			a[i+8] = numpy.int32(0)
			a[i+12] = numpy.int32(0)
			a[i+16] = numpy.int32(0)
			a[i+20] = numpy.int32(0)
			
		i += 1		
	a[24] = numpy.int32(data_halo)
	# i'm thinking about not use buffer range any more
	# but we can't change GPU immediately
	#a[25] = numpy.int32(buffer_halo)
	a[25] = numpy.int32(data_halo)
	
	return cuda.In(a)
global FD
FD = []


# FREYJA STREAMING #
# Handle intermediate function
# - Check data source and decide whether set on GPU mem or save in local directory
#def 



def run_function(function_package, function_name):
	# global variables
	global FD
	rf_time = time.time()
	
	# initialize variables
	fp = function_package
	func_output = fp.output
	u, ss, sp = func_output.get_id()
	data_halo = func_output.data_halo
	#print func_output.info()
	
	args = fp.function_args
	work_range = fp.work_range

	stream = stream_list[0]
	mod = source_module_dict[function_name]

	#cuda.memcpy_htod_async(bandwidth, numpy.float32(fp.TF_bandwidth), stream=stream)
	#cuda.memcpy_htod_async(front_back, numpy.int32(fp.front_back), stream=stream)

	#tf = mod.get_texref('TFF')	
	#tf1 = mod.get_texref('TFF1')
	#bandwidth,_ = mod.get_global('TF_bandwidth')
	
	import numpy
	if numpy.sum(fp.Sliders) != 0:
		sld,_ = mod.get_global('slider')
		sld_op,_ = mod.get_global('slider_opacity')

		cuda.memcpy_htod_async(sld, fp.Sliders, stream=stream)
		cuda.memcpy_htod_async(sld_op, fp.Slider_opacity, stream=stream)
	
	if fp.transN != 0:
		#tf = mod.get_texref('TFF')
		#tf1 = mod.get_texref('TFF1')
		#bandwidth,_ = mod.get_global('TF_bandwidth')

		#if fp.update_tf == 1 and fp.trans_tex != None:
			#global tfTex
			#tfTex = fp.trans_tex
		#if fp.update_tf2 == 1 and fp.trans_tex != None:
			#global tfTex2
			#tfTex2  = fp.trans_tex

		#tf.set_filter_mode(cuda.filter_mode.LINEAR)
		#tf1.set_filter_mode(cuda.filter_mode.LINEAR)
		#cuda.bind_array_to_texref(cuda.make_multichannel_2d_array(tfTex.reshape(1,256,4), order='C'), tf)
		#cuda.bind_array_to_texref(cuda.make_multichannel_2d_array(tfTex2.reshape(1,256,4), order='C'), tf1)
		#cuda.memcpy_htod_async(bandwidth, numpy.array(fp.TF_bandwidth,dtype=numpy.float32), stream=stream)
		pass


	# for plane copy
	if False:
		planes = numpy.ndarray((6, 4), dtype=numpy.float32)
		planes[0] = numpy.array([0,-1,0,128], dtype=numpy.float32)
		planes[1] = numpy.array([-1,0,0,150], dtype=numpy.float32)
		planes[2] = numpy.array([0,0,-1,150], dtype=numpy.float32)
		planes[3] = numpy.array([1,0,0,-50], dtype=numpy.float32)
		planes[4] = numpy.array([0,0,1,-50], dtype=numpy.float32)
		planes[5] = numpy.array([0,-1,0,180], dtype=numpy.float32)

		num_plane = 2;
		plane_idx = numpy.array([1,5], dtype=numpy.uint32)

		dev_plane_cnt,   _ = mod.get_global('plane_cnt')
		dev_plane_index, _ = mod.get_global('plane_index')
		dev_planes,      _ = mod.get_global('planes')

		cuda.memcpy_htod(dev_planes, planes)
		cuda.memcpy_htod(dev_plane_index, plane_idx)
		cuda.memcpy_htod(dev_plane_cnt, numpy.array(num_plane, dtype=numpy.uint32))
		
	# FREYJA STREAMING
	streaming_flag = function_package.stream
	if False:
		pass
	else:
		cuda_args = []
	
		data_exist = True
		if u not in data_list: data_exist = False
		elif ss not in data_list[u]: data_exist = False
		elif sp not in data_list[u][ss]: data_exist = False
	
		if data_exist:
			# initialize variables
			data_package = data_list[u][ss][sp]
			dp = data_package
	
			if dp.devptr == None:
				wait_data_arrive(data_package, stream=stream)
			###########################
			devptr = dp.devptr
			output_range = dp.data_range
			full_output_range = dp.full_data_range
			buffer_range = dp.buffer_range
			buffer_halo = dp.buffer_halo
			
			ad = data_range_to_cuda_in(output_range, full_output_range, buffer_range, buffer_halo=buffer_halo, stream=stream)
			cuda_args += [ad]
			output_package = dp
		
			FD.append(ad)
		else:
			#print_purple(func_output.info(), socket.gethostname())
			bytes = func_output.data_bytes
			devptr, usage = malloc_with_swap_out(bytes, debug_trace=True)
			log("created output data bytes %s"%(str(func_output.data_bytes)),'detail',log_type)
			data_range = func_output.data_range
			full_data_range = func_output.full_data_range
			buffer_range = func_output.buffer_range
			buffer_halo = func_output.buffer_halo

			#print_blue(func_output.info())
		
			ad = data_range_to_cuda_in(data_range, full_data_range, buffer_range, buffer_halo=buffer_halo, stream=stream)
	
			cuda_args += [ad]
			output_package = func_output
			output_package.set_usage(usage)
			
			if False:
				print "OUTPUT"
				print "OUTPUT_RANGE", data_range
				print "OUTPUT_FULL_RANGE",  full_data_range
				
			FD.append(ad)
	
		# set work range
		block, grid = range_to_block_grid(work_range)
		cuda_args = [devptr] + cuda_args
		
	#	print "GPU", rank, "BEFORE RECV", time.time()
		# Recv data from other process
		for data_package in args:
			u = data_package.get_unique_id()
			data_name = data_package.data_name
			if data_name not in work_range and u != '-1':
				wait_data_arrive(data_package, stream=stream)
	
	#	print "GPU", rank, "Recv Done", time.time()
		# set cuda arguments 
		for data_package in args:
			data_name = data_package.data_name
			data_dtype = data_package.data_dtype
			data_contents_dtype = data_package.data_contents_dtype

	
			u = data_package.get_unique_id()
			if data_name in work_range:
				cuda_args.append(numpy.int32(work_range[data_name][0]))
				cuda_args.append(numpy.int32(work_range[data_name][1]))
			elif u == '-1':
				data = data_package.data
				dtype = data_package.data_contents_dtype
				if dtype == 'int': 
					cuda_args.append(numpy.int32(data))
				elif dtype == 'float':
					cuda_args.append(numpy.float32(data))
				elif dtype == 'double':
					cuda_args.append(numpy.float64(data))
				else:
					cuda_args.append(numpy.float32(data)) # temp
			else:
				#print_green( data_package.info())
				ss = data_package.get_split_shape()
				sp = data_package.get_split_position()
				#print u, ss, sp
				if data_package.devptr == None:
					data_package = data_list[u][ss][sp] # it must be fixed to data_package later
	
				memory_type = data_package.memory_type
				#print "FROM %d\n"%(device_number),data_package.get_id()
				if memory_type == 'devptr':
	
					cuda_args.append(data_package.devptr)
					#print_green(socket.gethostname())
					#print_green(data_package.info())

					dirt_devptr(data_package.devptr)

					data_range = data_package.data_range
					full_data_range = data_package.full_data_range
					if False:
						print "DP", data_package.info()
						print_devptr(data_package.devptr, data_package)
					ad = data_range_to_cuda_in(data_range, full_data_range, data_halo=data_package.data_halo, stream=stream)
	
					cuda_args.append(ad)
					FD.append(ad)
	
		# set modelview matrix
		## !!!! FOR DAVID RENDERING
		if False:
			func = mod.get_function(function_name)
			mmtx,_ = mod.get_global('modelview')
			inv_mmtx, _ = mod.get_global('inv_modelview')
			filepath = '/home/whchoi/mvmtx/hoi/second/'  
			given_mmtx = numpy.fromstring(open(filepath+"/1.mvmtx",'r').read(), dtype=numpy.float32).reshape(4,4) 
			inv_m = numpy.linalg.inv(given_mmtx)
			cuda.memcpy_htod_async(mmtx, given_mmtx.reshape(16), stream=stream)
			cuda.memcpy_htod_async(inv_mmtx, inv_m.reshape(16), stream=stream)
	
		else:
			func = mod.get_function(function_name)
			mmtx,_ = mod.get_global('modelview')
			inv_mmtx, _ = mod.get_global('inv_modelview')
			inv_m = numpy.linalg.inv(fp.mmtx)
			cuda.memcpy_htod_async(mmtx, fp.mmtx.reshape(16), stream=stream)
			cuda.memcpy_htod_async(inv_mmtx, inv_m.reshape(16), stream=stream)
	
		stream_list[0].synchronize()
	
		if log_type in ['time','all']:
			start = time.time()
	
		
		st = time.time()
		kernel_finish = cuda.Event()
		func( *cuda_args, block=block, grid=grid, stream=stream_list[0])
		kernel_finish.record(stream=stream_list[0])
		ctx.synchronize()
		print "GPU TIME", time.time() - st
	#	print "FFFFOOo", func_output.info()
	#	print_devptr(cuda_args[0], func_output)
		u, ss, sp = func_output.get_id()
		target = (u,ss,sp)

		result_buffer = numpy.ndarray((1024,1024,4),dtype=numpy.uint8)
		cuda.memcpy_dtoh(result_buffer, cuda_args[0])
		import Image
		Image.fromarray(result_buffer[:,:,:3]).save("/home/freyja/result/%s.png"%(socket.gethostname()))
	
	
		Event_dict[target] = kernel_finish
		if target not in valid_list:
			valid_list.append(target)
	
		#################################################################################
		# finish
		if log_type in ['time','all']:
			t = (time.time() - start)
			ms = 1000*t
	
			log("rank%d, %s,  \"%s\", u=%d, GPU%d function running,,, time: %.3f ms "%(rank, func_output.data_name, function_name, u, device_number,  ms),'time',log_type)
		#log("rank%d, \"%s\", GPU%d function finish "%(rank, function_name, device_number),'general',log_type)
	

		#print "FROM ORIGINAL PROCESSING"
		#print "VVVV"
		###################################################################################
		# decrease retain_count
		for data_package in args:
			u = data_package.get_unique_id()
			if u != '-1':
				mem_release(data_package)
				u, ss, sp = data_package.get_id()

		return devptr, output_package 
# set GPU
device_number = rank-int(sys.argv[1])
#device_number = rank+3
cuda.init()
dev = cuda.Device(device_number)
ctx = dev.make_context()

# initialization
############################################################
data_list = {}
recv_list = {}

gpu_list = []
cpu_list = []
hard_list = []

dirty_flag = {}

mmtx = None

# stream
# memcpy use 1
stream_list = []

s_requests = []

valid_list = []
Event_dict = {}

func_dict = {}
function_code_dict = {} # function
function_and_kernel_mapping = {}
source_module_dict = {}

# data pools
data_pool = []
devptr_and_timestamp = []

numpy.set_printoptions(linewidth=200)

Debug = False
GPUDIRECT = True
VIVALDI_BLOCKING = False

## transfer function
tfTex = numpy.zeros(1024,dtype=numpy.uint8)
tfTex2 = numpy.zeros(1024,dtype=numpy.uint8)

log_type = False
n = 2
flag = ''
def init_stream():
	global stream_list
	for elem in range(2):
		stream_list.append(cuda.Stream())
init_stream()

attachment = load_GPU_attachment()
compile_for_GPU(None, kernel_function_name='default')

flag_times = {}
for elem in ["recv","send_order","free","memcpy_p2p_send","memcpy_p2p_recv","request_data","data_package","deploy_code","run_function","wait","update_data_loader"]:
	flag_times[elem] = 0
# execution
##########################################################

flag = ''
while flag != "finish":
	if log_type != False: print "GPU:", rank, "waiting"
	while not comm.Iprobe(source=MPI.ANY_SOURCE, tag=5):
		time.sleep(0.001)

	source = comm.recv(source=MPI.ANY_SOURCE,    tag=5)
	flag = comm.recv(source=source,              tag=5)

	#print_red("GPU_UNIT from %s #%s# by source : %s"%(socket.gethostname(),str(flag), source))


	if log_type != False: print "GPU:", rank, "source:", source, "flag:", flag 

	# interactive mode functions
	if flag == "log":
		log_type = comm.recv(source=source,    tag=5)
		print "GPU:",rank,"log type changed to", log_type
	elif flag == 'say':
		print "GPU hello", rank, name
	elif flag == 'get_function_list':
		if len(function_code_dict) == 0:
			print "GPU:",rank,"Don't have any function"
			continue
		for function_name in function_code_dict:
			print "GPU:", rank, "name:",name, "function_name:",function_name
	elif flag == "remove_function":
		name = comm.recv(source=source,    tag=5)
		if name in function_code_dict:
			print "GPU:",rank, "function:", name, "is removed"
			
			# remove source code
			try:
				del function_code_dict[name]
			except:
				print "Remove Failed1"
				print "function want to remove:", name
				print "exist function_list", function_code_dict.keys()
				continue
			
			# remove kernel function and source modules
			try:
				for kernel_function_name in function_and_kernel_mapping:
					del source_module_dict[kernel_function_name]
			except:
				print "Remove Failed2"
				print "function want to remove:", name
				print "exist function_list", function_and_kernel_mapping.keys()
				continue
			
			# remove mapping between function name and kernel_function_name
			try:
				if name in function_and_kernel_mapping:
					del function_and_kernel_mapping[name]
			except:
				print "Remove Failed3"
				print "function want to remove:", name
				print "exist function_list", function_and_kernel_mapping.keys()
				continue
		else:
			print "GPU:",rank, "No function named:", name
	elif flag == "get_data_list":
		print "GPU:", rank, "data_list:", data_list.keys()
	elif flag == "remove_data":
		uid = comm.recv(source=source,    tag=5)
		if uid in data_list:
			del data_list[uid]
			print "GPU:", rank, "removed data"
	# old functions
	if flag == "synchronize":
		# synchronize
	
		# requests wait
		f_requests = []
		for u in recv_list:
			for ss in recv_list[u]:
				for sp in recv_list[u][ss]:
					f_requests += [elem['request'] for elem in recv_list[u][ss][sp]]

		f_requests += [elem[0] for elem in s_requests]
		MPI.Request.Waitall(f_requests)

		# stream synchronize
		for stream in stream_list: stream.synchronize()

		# context synchronize
		ctx.synchronize()
		comm.send("Done", dest=source, tag=999)
	elif flag == "recv":
		st = time.time()
		data, data_package, request,_ = recv()
		dp = data_package
		notice(dp)
		save_data(data, dp)
		idle()
		flag_times[flag] += time.time() - st
	elif flag == "send_order":
		st = time.time()
		dest = comm.recv(source=source,    tag=53)
		u = comm.recv(source=source,       tag=53)
		ss = comm.recv(source=source,      tag=53)
		sp = comm.recv(source=source,      tag=53)


		log("rank%d, order to send data u=%d to %d"%(rank, u, dest),'general',log_type)

		data, data_package = data_finder(u,ss,sp, gpu_direct)
		dp = data_package
		ctx.synchronize()
		send(data, dp, dest=dest, gpu_direct=gpu_direct)
		idle()
		flag_times[flag] += time.time() - st
	elif flag == "free":
		st = time.time()
		u = comm.recv(source=source,            tag=58)
		ss = comm.recv(source=source,           tag=58)
		sp = comm.recv(source=source,           tag=58)
		data_halo = comm.recv(source=source,    tag=58)

		# check this data is really exsit
		if u not in data_list: continue
		if ss not in data_list[u]: continue
		if sp not in data_list[u][ss]: continue

		# data exist
		if log_type in ['memory']:
			log_str = "RANK%d, FREE BF "%(rank) + str(cuda.mem_get_info()) + " %s %s %s"%(u, ss, sp)
			log(log_str, 'memory', log_type)
		
		target = (u,ss,sp)

		if target in gpu_list:
			gpu_list.remove(target)
#			data_list[u][ss][sp].devptr.free()

			if data_list[u][ss][sp].usage == 0:
				print "Vivaldi system error"
				print "--------------------------------"
				print "Not correct 'usage' value"
				print data_list[u][ss][sp]
				print "--------------------------------"
				assert(False)
			data_pool_append(data_pool, data_list[u][ss][sp].devptr, data_list[u][ss][sp].usage)
#			print "APPEND C", len(data_pool), rank
#			print data_list[u][ss][sp].data_bytes, data_list[u][ss][sp].usage

			del(data_list[u][ss][sp])
			if data_list[u][ss] == {}: del(data_list[u][ss])
			if data_list[u] == {}: del(data_list[u])
		else:
			assert(False)

		if target in cpu_list: cpu_list.remove(target)
		if target in hard_list: hard_list.remove(target)

		if log_type in ['memory']:
			log_str = "RANK%d, FREE AF "%(rank)+ str(cuda.mem_get_info())+ " %s %s %s"%(u, ss, sp)
			log(log_str, 'memory', log_type)

		flag_times[flag] += time.time() - st
	elif flag == "memcpy_p2p_send":
		st = time.time()
		dest = comm.recv(source=source,    tag=56)
		task = comm.recv(source=source,    tag=56)

		memcpy_p2p_send(task, dest)
		idle()
		flag_times[flag] += time.time() - st
	elif flag == "memcpy_p2p_recv":
		st = time.time()
		task = comm.recv(source=source,         tag=57)
		data_halo = comm.recv(source=source,    tag=57)

		#if data_halo > 0:
		memcpy_p2p_recv(task, data_halo)

		# FREYJA STREAMING
		#else:
			#notice(task.dest)


		mem_release(task.source)
		idle()
		flag_times[flag] += time.time() - st
	elif flag == "request_data":
		st = time.time()
		u = comm.recv(source=source,   tag=55)
		ss = comm.recv(source=source,  tag=55)
		sp = comm.recv(source=source,  tag=55)
		log("rank%d, send data u=%d to %d"%(rank, u, source),'general',log_type)
		gpu_direct = comm.recv(source=source, tag=52)
		data, data_package = data_finder(u,ss,sp, gpu_direct)
		send(data, data_package, dest=source, gpu_direct=gpu_direct)
		flag_times[flag] += time.time() - st
	elif flag == "finish":
		disconnect()
	elif flag == 'set_function':
		# x can be bunch of functions or function
		x = comm.recv(source=source, tag=5)
		new_function_code_dict = get_function_dict(x)
		function_code_dict.update(new_function_code_dict)
	elif flag == "run_function":
		function_package = comm.recv(source=source, tag=51)

		try:
			# get kernel function name
			def get_kernel_function_name(function_package):
				kernel_function_name = function_package.function_name
				function_args = function_package.function_args
				for elem in function_args:
					if elem.data_dtype == numpy.ndarray:
						kernel_function_name += elem.data_contents_dtype
				return kernel_function_name		
			kernel_function_name = get_kernel_function_name(function_package)

			# compile if kernel function not exist
			if kernel_function_name not in func_dict:
				compile_for_GPU(function_package, kernel_function_name)
				# add mapping between function name and kernel function
				function_name = function_package.function_name
				if function_name not in function_and_kernel_mapping:
					function_and_kernel_mapping[function_name] = []
				if kernel_function_name not in function_and_kernel_mapping[function_name]:
					function_and_kernel_mapping[function_name].append(kernel_function_name)

			# update dirty flag for devptr
			for elem in function_package.function_args:
				if elem.devptr != None:
					update_devptr_timestamp(devptr)
					
					
			devptr, output_package = run_function(function_package, kernel_function_name)
			
			notice(function_package.output)
			#if not output_package.stream:
			save_data(devptr, output_package)
			if output_package.data_source == 'local':
				dirt_devptr(devptr)
				save_as_local(output_package, devptr)
				#print "HOI"
				pass
		except:
			print "DAMN, %s"%socket.gethostname()
			exc_type, exc_value, exc_traceback = sys.exc_info()
			a = ''.join(traceback.format_exception(exc_type, exc_value, exc_traceback))
			print a
		idle()
	elif flag == "update_data_loader":
		data_loader_list = comm.recv(source=source, tag=61)
		#print_red(data_loader_list)

	#print_blue("DODONE %s %s"%(flag, socket.gethostname()))
