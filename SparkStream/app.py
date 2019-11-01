import socket
import sys
import requests
import json
import time
import csv

def send_data_to_spark(tcp_connection,reader):
	count=0
	for row in reader:
		finstr=''
		for j in row:
			if(finstr!=''):
				finstr=finstr+','+j
			else:
				finstr=finstr+j
		finstr=finstr+'\n'
		count=count+1
		tcp_connection.send(finstr.encode())
		if(count==2000):
			# tcp_connection.send(finstr.encode())
			time.sleep(2)
			count=0
	# tcp_connection.close()

TCP_IP="localhost"
TCP_PORT=9009
conn=None
s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
s.bind((TCP_IP,TCP_PORT))	

csvfile=open('FIFA_modded_small.csv','r')
fieldnames = ("ID","language","Date","source","len","bool", "likes","Hashtags","Data1","Data2","Data3","Data4")
reader=csv.reader(csvfile,fieldnames)
time.sleep(2)

s.listen(1)
print("Waiting for connection...")

conn,addr=s.accept()
print("Connected... Starting to get twitter data")

send_data_to_spark(conn,reader)
