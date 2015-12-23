#!/usr/bin/env python

import socket
import time
import platform
import math

class carbon_api:

	def __init__(self):
		print "Initializing..."
		self.CARBON_SERVER = '209.243.176.192'
		self.CARBON_PORT = 2003
		sock = socket.socket()
		sock.connect((self.CARBON_SERVER, self.CARBON_PORT))
		sock.close()
		print "\tConnected to the cluster..."
		
	def send_msg(self,message):
		print 'sending message:\n%s' % message
		sock = socket.socket()
		sock.connect((self.CARBON_SERVER, self.CARBON_PORT))
		result=sock.sendall(message)
		sock.close()
		return result==None
	
	"""
				Device:         rrqm/s   wrqm/s     r/s     w/s   rsec/s   wsec/s avgrq-sz avgqu-sz   await  svctm  %util
	EXAMPLE: 	sdb               0.01   522.30   31.14   22.49  4268.06  4424.59   162.10     0.11    2.10   0.52   2.77
	system text,
	device text,
	rrqm FLOAT, The number of read requests merged per second  that  were issued to the device.
	wrqm FLOAT, The  number of write requests merged per second that were issued to the device.
	reads FLOAT, The number of read  requests  that  were  issued  to  the device per second.
	writes FLOAT, The  number  of  write  requests  that were issued to the device per second.
	rsec FLOAT, The number of sectors read from the device per second.
	wsec FLOAT, The number of sectors written to the device per second.
	avgrqsz FLOAT, The average size (in sectors) of the requests  that  were issued to the device.
	avgqusz FLOAT, The average queue length of the requests that were issued to the device.
	await FLOAT, The average  time  (in  milliseconds)  for  I/O  requests
                     issued to the device to be served. This includes the time
                     spent by the requests in queue and the time spent servic-
                     ing them.
	svctm FLOAT, The  average  service  time  (in  milliseconds)  for  I/O
                     requests that were issued to the device.
	util FLOAT, Percentage of CPU time during  which  I/O  requests  were
                     issued  to  the  device  (bandwidth  utilization  for the
                     device). Device saturation  occurs  when  this  value  is
                     close to 100%.
	
	iostat_results = system, device, rrqm, wrqm, reads, writes, rsec, wsec, avgrqsz, avgqusz, await, svctm, util
	"""
	def insert_iostat_system_device(self,iostat_results):
		"""
		iostat.<system>.total.
			|rrqm - SUM
			|wrqm - SUM
			|reads - SUM
			|writes - SUM
			|rsec - SUM
			|wsec - SUM
			|avgrqsz.
				|1|2|4|8|16|32|64|128|256|512|1024 - For Each Drive: 100*(x.avgrqsz)*(x.reads+x.writes) / (total.reads + total.writes) - STACKED
			|avgqusz.
				|1|2|4|8|16|32|64|128|256 - For Each Drive: 100*(x.avgqusz) / (total number of drives) - STACKED
			|await - AVERAGE
			|svctm - AVERAGE
			|util - AVERAGE
		iostat.<system>.<device>.
			|rrqm - raw
			|wrqm - raw
			|reads - raw
			|writes - raw
			|rsec - raw
			|wsec - raw
			|avgrqsz - raw
			|avgqusz - raw
			|await - raw
			|svctm - raw
			|util - raw
		"""
		total_rrqm=0
		total_wrqm=0
		total_reads=0
		total_writes=0
		total_rsec=0
		total_wsec=0
		total_avgrqsz=[0]*11
		total_avgqusz=[0]*9
		total_await=0
		count_await=0
		total_svctm=0
		count_svctm=0
		total_util=0
		count_util=0
	
		node = platform.node().replace('.', '-')
		timestamp = int(time.time())
		for device in iostat_results:
			total_rrqm += float(device[2])
			total_wrqm += float(device[3])
			total_reads += float(device[4])
			total_writes += float(device[5])
			total_rsec += float(device[6])
			total_wsec += float(device[7])
			total_await += float(device[10])
			count_await += 1
			total_svctm += float(device[11])
			count_svctm += 1
			total_util += float(device[12])
			count_util += 1
		
		for device in iostat_results:
			device_reads = float(device[4])
			device_writes = float(device[5])
			
			#Average Request Size
			device_avgrqsz = float(device[8])
			log_avgrqsz = 0
			if device_avgrqsz >= 1.0:
				log_avgrqsz = int(math.log(device_avgrqsz,2))
				if log_avgrqsz > len(total_avgrqsz)-1:
					log_avgrqsz = len(total_avgrqsz)-1
			total_avgrqsz[log_avgrqsz]+= 100.0 * (device_reads+device_writes) / (total_reads+total_writes+1)
			
			#Average Queue Size
			device_avgqusz = float(device[9])
			log_avgqusz = 0
			if device_avgqusz >= 1.0:
				log_avgqusz = int(math.log(device_avgqusz+1.0,2))
				if log_avgqusz > len(total_avgqusz)-1:
					log_avgqusz = len(total_avgqusz)-1
			total_avgqusz[log_avgqusz]+= 100.0 / float(len(iostat_results))
			
		if len(iostat_results)>0:
			total_await = total_await / count_await
			total_svctm = total_svctm / count_svctm
			total_util = total_util / count_util
			
			# Total
			lines=[]
			lines.append('iostat.%s.total.rrqm %s %d' % (node, total_rrqm, timestamp))
			lines.append('iostat.%s.total.wrqm %s %d' % (node, total_wrqm, timestamp))
			lines.append('iostat.%s.total.reads %s %d' % (node, total_reads, timestamp))
			lines.append('iostat.%s.total.writes %s %d' % (node, total_writes, timestamp))
			lines.append('iostat.%s.total.rsec %s %d' % (node, total_rsec, timestamp))
			lines.append('iostat.%s.total.wsec %s %d' % (node, total_wsec, timestamp))
			lines.append('iostat.%s.total.await %s %d' % (node, total_await, timestamp))
			lines.append('iostat.%s.total.svctm %s %d' % (node, total_svctm, timestamp))
			lines.append('iostat.%s.total.util %s %d' % (node, total_util, timestamp))
			for y in range(0,len(total_avgrqsz)):
				lines.append('iostat.%s.total.avgrqsz.%d %s %d' % (node, int(math.pow(2,y)), total_avgrqsz[y], timestamp))
			for y in range(0,len(total_avgqusz)):
				lines.append('iostat.%s.total.avgqusz.%d %s %d' % (node, int(math.pow(2,y-1)), total_avgqusz[y], timestamp))
			message = '\n'.join(lines) + '\n'
			self.send_msg(message)
		
		# Per Device
		for device in iostat_results:
			lines=[]
			drive=device[1]
			lines.append('iostat.%s.%s.rrqm %s %d' % (node, drive, device[2], timestamp))
			lines.append('iostat.%s.%s.wrqm %s %d' % (node, drive, device[3], timestamp))
			lines.append('iostat.%s.%s.reads %s %d' % (node, drive, device[4], timestamp))
			lines.append('iostat.%s.%s.writes %s %d' % (node, drive, device[5], timestamp))
			lines.append('iostat.%s.%s.rsec %s %d' % (node, drive, device[6], timestamp))
			lines.append('iostat.%s.%s.wsec %s %d' % (node, drive, device[7], timestamp))
			lines.append('iostat.%s.%s.avgrqsz %s %d' % (node, drive, device[8], timestamp))
			lines.append('iostat.%s.%s.avgqusz %s %d' % (node, drive, device[9], timestamp))
			lines.append('iostat.%s.%s.await %s %d' % (node, drive, device[10], timestamp))
			lines.append('iostat.%s.%s.svctm %s %d' % (node, drive, device[11], timestamp))
			lines.append('iostat.%s.%s.util %s %d' % (node, drive, device[12], timestamp))
			message = '\n'.join(lines) + '\n'
			self.send_msg(message)
		
	"""
	system text,
	serial text,
	status INT, - 0 is OK, 1 is not OK! SMART Health Status
	endurance INT, - SS Media used endurance indicator
	temp INT, - Current Drive Temperature
	defects BIGINT, - Elements in grown defect list
	errors BIGINT, - Non-medium error count
	
	smart_results = system, serial, status, endurance, temp, defects, errors
	"""
	def insert_smart_system_serial(self,smart_results):
		"""
		smart.<system>.<serial>.
			|status - raw
			|endurance - raw
			|temp - raw
			|defects - raw
			|errors - raw
		"""
		# Per Device
		node = platform.node().replace('.', '-')
		timestamp = int(time.time())
		lines=[]
		serial=smart_results[1]
		lines.append('smart.%s.%s.status %s %d' % (node, serial, smart_results[2], timestamp))
		lines.append('smart.%s.%s.endurance %s %d' % (node, serial, smart_results[3], timestamp))
		lines.append('smart.%s.%s.temp %s %d' % (node, serial, smart_results[4], timestamp))
		lines.append('smart.%s.%s.defects %s %d' % (node, serial, smart_results[5], timestamp))
		lines.append('smart.%s.%s.errors %s %d' % (node, serial, smart_results[6], timestamp))
		message = '\n'.join(lines) + '\n'
		self.send_msg(message)
		
	"""
	system
	cpu_user - user level (application).
	cpu_nice - user level with nice priority.
	cpu_system - system level (kernel).
	cpu_iowait - Percentage of time that the CPU or CPUs were idle during which the system had an outstanding disk I/O request.
	cpu_steal - Percentage of time a virtual CPU waits for a real CPU while the hypervisor is servicing another virtual processor.
	cpu_idle - Percentage of time that the CPU or CPUs were idle and the system did not have an outstanding disk I/O request.
	mem_total
	mem_used
	mem_free
	mem_shared
	mem_buffers
	mem_cached
	
	perfmon_results = system,cpu_user,cpu_nice,cpu_system,cpu_iowait,cpu_steal,cpu_idle,mem_total,mem_used,mem_free,mem_shared,mem_buffers,mem_cached
	"""
	def insert_perfmon_system(self,perfmon_results):
		"""
		perfmon.<system>.
			|... - raw
		"""
		# Per System
		node = platform.node().replace('.', '-')
		timestamp = int(time.time())
		lines=[]
		lines.append('perfmon.%s.cpu_user %s %d' % (node, perfmon_results[0], timestamp))
		lines.append('perfmon.%s.cpu_nice %s %d' % (node, perfmon_results[1], timestamp))
		lines.append('perfmon.%s.cpu_system %s %d' % (node, perfmon_results[2], timestamp))
		lines.append('perfmon.%s.cpu_iowait %s %d' % (node, perfmon_results[3], timestamp))
		lines.append('perfmon.%s.cpu_steal %s %d' % (node, perfmon_results[4], timestamp))
		lines.append('perfmon.%s.cpu_idle %s %d' % (node, perfmon_results[5], timestamp))
		lines.append('perfmon.%s.mem_total %s %d' % (node, perfmon_results[6], timestamp))
		lines.append('perfmon.%s.mem_used %s %d' % (node, perfmon_results[7], timestamp))
		lines.append('perfmon.%s.mem_free %s %d' % (node, perfmon_results[8], timestamp))
		lines.append('perfmon.%s.mem_shared %s %d' % (node, perfmon_results[9], timestamp))
		lines.append('perfmon.%s.mem_buffers %s %d' % (node, perfmon_results[10], timestamp))
		lines.append('perfmon.%s.mem_cached %s %d' % (node, perfmon_results[11], timestamp))
		message = '\n'.join(lines) + '\n'
		self.send_msg(message)
