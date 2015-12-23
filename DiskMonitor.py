"""
# ----------------------------------------------------------------------------------------------
# Author: Remington Brasga
# Create Date:   August 3, 2015
#
# Description: Collect SMART data and other statistics.
----------------------------------------------------------------------------------------------
"""

import os, sys, time
import datetime
import csv
from subprocess import Popen,PIPE
from carbon_api import carbon_api
import socket

device_list = dict() #Add custom devices not found by `smartctl --scan`
WAIT_TIME=3600
IOSTAT_TIME=60
CarbonAPI = carbon_api()

def execute():
	# Get the paths of all of the devices (/dev/sda, /dev/sdb,... /dev/sd*)
	print("Building Device List")
	f = os.popen('smartctl --scan')
	output = f.read()
	output_array = output.split("\n")
	for line in output_array:
		if "/dev/" in line:
			devicepath=line.split()[0]
			device=devicepath.split("/")[2]
			device_list[device]=""
			#print "device:", device
	# Turn on SMART Data Collection
	print("Enabling SMART")
	index = 0
	for device in device_list.keys():
		percent_complete = ((100*index)/len(device_list))
		sys.stdout.write("%d%% complete\t|\t%s\r" % (percent_complete,device))
		sys.stdout.flush()
		os.popen('smartctl --smart=on --offlineauto=on --saveauto=on /dev/%s' % device)
		index += 1
	print("[DONE]\t\t\t\t\t\r")
	
	try:
		print "Beginning Real-Time Monitor..."
		current = time.time()
		while True:
			current = time.time()
			ssd_stats = []
			# Get SMART Data
			print("Recording SMART Data")
			index = 0
			for device in device_list.keys():
				vendor = ""
				product_id = ""
				serial = ""
				percent_complete = ((100*index)/len(device_list))
				sys.stdout.write("%d%% complete\t|\t%s\r" % (percent_complete,device))
				sys.stdout.flush()
				f = os.popen('smartctl -a /dev/%s' % device)
				text=f.read()
				vendor,product_id,serial,str_time = processSMART(text)
				device_list[device]=serial
				print("[DONE]\t\t\t\t\t\r")
				index += 1
			print "[INFO] Sleeping until the next minute..."
			while int(current+WAIT_TIME) > int(time.time()):
				iostat_time = time.time()
				processIOSTAT()
				processPerf()
				while int(iostat_time+IOSTAT_TIME) > int(time.time()):
					time.sleep(1)
	except:
		print "Unexpected error:", sys.exc_info()[0]
		raise

def processSMART(text):
	# Process the SMART text
	tmplines = text.split("\n")
	# Primary Keys
	system=socket.gethostname()
	vendor="" #check
	product_id="" #check
	serial="" #check
	long_time = 0 #check
	revision="" #check
	user_capacity="" #check
	block_size="" #check
	unit_id="" #check
	device_type="None" #check
	protocol="None" #check
	trip_temp=0 #check
	manufactured="N/A"
	str_time="" #check
	status="0"
	endurance=0
	temp=0 #check
	defects=0
	errors=0
	
	for line in tmplines:
		#print "Line!!!", line
		if line.find("Vendor") > -1 and vendor=="":
			tmpline = line.split()
			tmpline.pop(0)
			vendor=tmpline[0]
			#print "Vendor:", vendor
		elif line.find("Product") > -1:
			tmpline = line.split()
			tmpline.pop(0)
			product_id=tmpline[0]
			#print "Product:", product_id
		elif line.find("Serial") > -1:
			serial_line = line.split()
			for x in range(0,len(serial_line)):
				if serial_line[x].find(":") > -1:
					serial = serial_line[x+1]
					#print "Serial:", serial
		elif line.find("Time") > -1 and str_time == "":
			tmpline = line.split()
			tmpline.pop(0)
			tmpline.pop(0)
			tmpline.pop(0)
			for x in range(0,len(tmpline)):
				str_time = str_time + tmpline[x] + " "
			#print "Time:", str_time
		elif line.find("Firmware Version") > -1:
			tmpline = line.split()
			tmpline.pop(0)
			tmpline.pop(0)
			revision=tmpline[0]
			#print "Revision:", revision 
		elif line.find("Revision") > -1:
			tmpline = line.split()
			tmpline.pop(0)
			revision=tmpline[0]
			#print "Revision:", revision 
		elif line.find("Capacity") > -1:
			tmpline = line.split()
			tmpline.pop(0)
			tmpline.pop(0)
			user_capacity=str(tmpline)
			#print "User Capacity:", user_capacity
		elif line.find("block size") > -1 or line.find("Sector Size") > -1:
			tmpline = line.split()
			tmpline.pop(0)
			tmpline.pop(0)
			tmpline.pop(0)
			for x in range(0,len(tmpline)):
				block_size = block_size + tmpline[x] + " "
			#print "Block Size:", block_size
		elif line.find("Unit id") > -1:
			tmpline = line.split()
			tmpline.pop(0)
			tmpline.pop(0)
			tmpline.pop(0)
			unit_id=tmpline[0]
			#print "Unit ID:", unit_id
		elif line.find("Device type") > -1:
			tmpline = line.split()
			tmpline.pop(0)
			tmpline.pop(0)
			device_type=tmpline[0]
			#print "Device type:", device_type 
		elif line.find("Transport protocol") > -1:
			tmpline = line.split()
			tmpline.pop(0)
			tmpline.pop(0)
			protocol=tmpline[0]
			#print "Transport Protocol:", protocol
		elif line.find("Manufactured") > -1:
			tmpline = line.split()
			tmpline.pop(0)
			tmpline.pop(0)
			manufactured=""
			for x in range(0,len(tmpline)):
				manufactured = manufactured + tmpline[x] + " "
			#print "Manufactured:", manufactured
		elif line.find("Health Status") > -1:
			tmpline = line.split()
			tmpline.pop(0)
			tmpline.pop(0)
			tmpline.pop(0)
			if "OK" not in tmpline[0]:
				status="1"
			#print "Status:", status
		elif line.find("endurance indicator") > -1:
			tmpline = line.split()
			tmpline.pop(0)
			tmpline.pop(0)
			tmpline.pop(0)
			tmpline.pop(0)
			tmpline.pop(0)
			endurance=int(tmpline[0].strip("%%"))
			#print "Endurance:", endurance
		elif line.find("grown defect list") > -1:
                        tmpline = line.split()
			tmpline.pop(0)
			tmpline.pop(0)
			tmpline.pop(0)
			tmpline.pop(0)
			tmpline.pop(0)
			defects=int(tmpline[0])
			#print "Defects:", defects
		elif line.find("Non-medium error count") > -1:
                        tmpline = line.split()
			tmpline.pop(0)
			tmpline.pop(0)
			tmpline.pop(0)
			errors=int(tmpline[0])
			#print "Errors:", errors
		elif line.find("Drive Trip Temperature") > -1:
			tmpline = line.split()
			for x in range(0,len(tmpline)):
				if tmpline[x].find(":") > -1:
					str_temp = tmpline[x+1]
					trip_temp = int(str_temp)
					#print "Drive Trip Temperature:", trip_temp, "C"
		elif line.find("Current Drive Temperature") > -1:
			tmpline = line.split()
			for x in range(0,len(tmpline)):
				if tmpline[x].find(":") > -1:
					str_temp = tmpline[x+1]
					temp = int(str_temp)
					#print "Current Drive Temperature:", temp, "C"
		# Corner Cases
		elif line.find("Model Family") > -1:
			tmpline = line.split()
			tmpline.pop(0)
			tmpline.pop(0)
			vendor=str(tmpline)
			#print "Vendor:", vendor
		elif line.find("Device Model") > -1:
			tmpline = line.split()
			tmpline.pop(0)
			tmpline.pop(0)
			if vendor=="" and len(tmpline)>=2:
				vendor=tmpline.pop(0)
			else:
				vendor="OCZ"
			product_id=tmpline.pop(0)
			#print "Vendor:", vendor
	
	# Example: str_time = "Wed Aug 12 13:21:58 2015 PDT"
	# Process the time
	#print "Processing Time", str_time
	a = time.strptime(str_time,'%a %b %d %H:%M:%S %Y %Z ')
	str_time = time.strftime('%Y-%m-%d %H:%M:%S+0800',a)
	long_time = long(time.mktime(a))*long(1000)
	# str_hour = time.strftime("%H", a)
	# int_hour = int(str_hour)
	system=socket.gethostname()
	smart_results = [system, serial, status, endurance, temp, defects, errors]
	CarbonAPI.insert_smart_system_serial(smart_results)
	return vendor,product_id,serial,long_time
	
def processIOSTAT():
	iostat_results=[]
	long_time = long(time.mktime(time.localtime()))*long(1000)
	for device in device_list.keys():
		f = os.popen('iostat -x 1 1 -y /dev/%s' % device)
		text=f.read()
		# Process the SMART text
		tmplines = text.split("\n")
		# Primary Keys
		system=socket.gethostname()
		deviceFound = False
		for line in tmplines:
			if deviceFound:
				tmpline = line.split()
				if len(tmpline) == 12:
					serial=device_list[device]
					name = serial
					if name == "":
						name = tmpline[0]
					device_result=[system,name,tmpline[1],tmpline[2],tmpline[3],tmpline[4],tmpline[5],tmpline[6],tmpline[7],tmpline[8],tmpline[9],tmpline[10],tmpline[11]]
					iostat_results.append(device_result)
				else:
					print "[ERROR] iostat data does not contain exactly 12 elements"
					print "Length: ", len(tmpline), ", Array: ", tmpline
				deviceFound=False
			elif line.find("Device") > -1:
				deviceFound = True
	CarbonAPI.insert_iostat_system_device(iostat_results)
	#print "System, Device, Time:", system, device, epoch_time
	
	
def processPerf():
	perfmon_results=[]
	long_time = long(time.mktime(time.localtime()))*long(1000)
	system=socket.gethostname()
	f = os.popen('iostat -x 1 1 -y /dev/sda')
	text=f.read()
	# Process the SMART text
	tmplines = text.split("\n")
	# Primary Keys
	cpuFound = False
	for line in tmplines:
		if cpuFound:
			tmpline = line.split()
			if len(tmpline) == 6:
				perfmon_results.extend(tmpline)
			else:
				print "[ERROR] perfmon 'avg-cpu' data does not contain exactly 6 elements"
				print "Length: ", len(tmpline), ", Array: ", tmpline
			cpuFound=False
		elif line.find("avg-cpu:") > -1:
			cpuFound = True
	
	f = os.popen('free')
	text=f.read()
	# Process the SMART text
	tmplines = text.split("\n")
	# Primary Keys
	for line in tmplines:
		if line.find("Mem:") > -1:
			tmpline = line.split()
			if len(tmpline) == 7:
				perfmon_results.extend([tmpline[1],tmpline[2],tmpline[3],tmpline[4],tmpline[5],tmpline[6]])
			else:
				print "[ERROR] perfmon 'free' data does not contain exactly 7 elements"
				print "Length: ", len(tmpline), ", Array: ", tmpline
	CarbonAPI.insert_perfmon_system(perfmon_results)
	
if __name__ == "__main__":
	execute()

