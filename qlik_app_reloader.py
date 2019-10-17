# IMPORTS
import json
import sys
from websocket import create_connection
import ssl
sys.path.append("<redacted>") # Directory for Python classes
from gpconnect import GP_Connect, Timestamp_Update, Log_Table, Qlik_Connect #additional classes for added functionality
import multiprocessing
from multiprocessing import Pool, Value
import datetime
from random import randint

counter = None

#WEBSOCKET INFO
folder = "<redacted>"
certs = ({"ca_certs": folder + "root.pem",
            "certfile": folder + "client.pem",
            "keyfile": folder + "client_key.pem",
            "cert_reqs":ssl.CERT_NONE,
            "server_side": False
            })

#CLASS FOR POOL PROCESSING
class Reloader(object):

	def __init__(self, path):
		
		self.n = randint(5,6) #RANDOME INT (5 or 6) TO SWITCH BETWEEN THE TWO AVAILABLE IP ADDRESSES
		self.url = "<redacted>"+str(self.n)+":4747/app/"+path #CREATES UNIQUE WEBSOCKET CONNECTION FOR EACH APP TO BE RELOADED
		self.error = True
		
		#FOR LOGGING DURATION
		self.start_time = datetime.datetime.utcnow()
		self.end_time = datetime.datetime.utcnow()
		
		#JSON REQUEST CREATION AND FULL/PARTIAL RELOAD VARIABLE
		self.request ={}
		self.full = False
		
		#WEBSOCKET CREATION
		userDirectory, userId = "INTERNAL","apiuser"
		ssl.match_hostname = lambda cert, hostname: True
		self.ws = create_connection(self.url, sslopt=certs, header={'X-Qlik-User: UserDirectory=%s; UserId=%s'% (userDirectory, userId)})
		
		#ADDITIONAL BASE VARIABLES FOR LATER USE IN THIS CLASS
		self.path = ""
		self.string = ""
		
	def main(self,path, full):
		
		#REDUNDANTLY DEFINES BOTH PATH(UUID) AND FULL/PARTIAL RELOAD
		self.full = full
		self.path = path
		
		#BASE JSON REQUEST
		self.request ={
				"jsonrpc": "2.0",
				"id": 0,
				"method": "OpenDoc",
				"handle": -1,
				"params": [
					path,
					""
				]
			}
		
		#DEFINES START TIME FOR DURATION
		self.start_time = datetime.datetime.utcnow()
		
		#BEGINS PROCESSING
		self.open_app()
				
	def open_app(self):
		
		#OPENS APP IN QLIKSENSE API ENGINE
		try:
			#SENDS JSON REQUEST
			self.ws.send(json.dumps(self.request))
			
			#REQUEST RESPONSE GATHERING
			response = self.ws.recv()
			response = json.loads(self.ws.recv())
			
			self.check_app()
			
		except Exception as e:
			print("Error while opening the app.")
			print(e)
			self.close(True) #True TELLS close() THERE HAS BEEN AN ERROR
	
	def check_app(self):
		
		#CHECKS THE APP PREVIOUSLY REQUESTED
		try:
			#MODIFIES BASE JSON REQUEST
			request = self.request
			request["id"]=1
			request["method"]="GetActiveDoc"
			request["params"]=[]
		
			#SENDS REQUEST
			self.ws.send(json.dumps(request))
			
			#REQUEST RESPONSE GATHERING
			response = json.loads(self.ws.recv())
			self.reload_app()
		
		except Exception as e:
			print("Error while checking app status.")
			print(e)
			self.close(True) #True TELLS close() THERE HAS BEEN AN ERROR
					
	def reload_app(self):
		
		#RELOADS APP 
		try:
			#FULL RELOAD
			request = self.request
			request["id"]=2
			request["method"]="DoReloadEx"
			request["params"]={}
			request["handle"]=1
			
			if self.full == False: #BASED ON SQL TABLE
				#PARTIAL RELOAD
				request["params"] = [{"qMode": 2, "qPartial": True, "qDebug": False}]
			
			#SENDS MODIFIED REQUEST AND SAVES RESPONSE
			self.ws.send(json.dumps(request))
			response =  json.loads(self.ws.recv())
			
			self.count_up() # COUNTER FOR JOB NUMBER VS TOTAL JOBS
			
			#CREATES OUTPUT BASED ON RESPONSE FROM API ENGINE
			# IF FAILURE RESPONSE
			if response['result']['qResult']['qSuccess']== False:
				logFile = response["result"]["qResult"]["qScriptLogFile"] #GATHERS LOGFILE INFO			
				self.string += str(" FAILED " +logFile) #PRINTS LOGFILE INFO
				
			#IF SUCCESS RESPONSE
			elif response['result']['qResult']['qSuccess']== True: 
				logFile = response["result"]["qResult"]["qScriptLogFile"] #GATHERS LOGFILE INFO
				self.string += str(" Success "+"80.88.187.4"+str(self.n)+" - " + logFile) #PRINTS LOGFILE INFO

			self.save_app()
		
		except Exception as e:
			print("Error while starting reload.")
			print(e)
			self.close(True) #True TELLS close() THERE HAS BEEN AN ERROR

		#CHECKS RELOAD STATUS
		try:
			status = response["result"]["qResult"]["qSuccess"]

		except:
			print("Error. Reload failed.")
			try:
				logFile = response["result"]["qResult"]["qScriptLogFile"] #GATHERS LOGFILE INFO
				print("See log at ", logFile) #PRINTS LOGFILE INFO
			except:
				print("No log was generated")
				
			self.close(True) #True TELLS close() THERE HAS BEEN AN ERROR
	
	def save_app(self):
		
		#SAVES APP
		try: #JSON REQUEST MODIFICATION
			request = self.request
			request["id"] = 6
			request["method"] = "DoSave"
			request["handle"] = -1
			request["params"] = []
			
			#SENDS MODIFIED REQUEST AND SAVES RESPONSE
			self.ws.send(json.dumps(request))
			response =  json.loads(self.ws.recv())
			
			self.close()
			
		except Exception as e:
			print("Error while saving the app.")
			print(e)
			self.close(True) #True TELLS close() THERE HAS BEEN AN ERROR
	
	def close(self, err=False):
		
		#LOGS END TIME FOR DURAION
		self.end_time = datetime.datetime.utcnow()
		
		#LOGS ALL INFO TO SQL TABLE
		l = Log_Table().main(self.path, self.start_time, self.end_time, self.full, err)
		
		#PRINTS ALL INFO GATHERED IN PROCESS
		print(self.string)
		
		#CLOSES WEBSOCKET CONNECTION
		self.ws.close()
		
		#RETURNS LOGTABLE DATA IN CASE OF LATER NEED
		return l

	def count_up(self): #COUNTER FOR JOB/TOTAL_JOBS

		global total_jobs
		global counter	
		with counter.get_lock():
			counter.value += 1
			self.string += str(counter.value)
			self.string += str("/")
			self.string += str(total_jobs)

def process(paths): #PROCESS CREATION METHOD

	app1 = Reloader(paths[0])
	app1.main(paths[0],paths[1])
	
	if app1.main: #IF RELOADER SUCCESSFUL, UPDATES TIMESTAMPS
		t.main(paths[0])
		
		
if __name__=="__main__":
	
	counter = Value('i',0) #JOB COUNTER
	start_time = datetime.datetime.utcnow() #START TIME

	#GREENPLUM CONNECTION AND TIMESTAMP
	gp = GP_Connect()
	t = Timestamp_Update()
	
	#CREATES LIST FROM SQL TABLE AND DETERMINES # OF JOBS
	paths = gp.main(command = "SELECT id, full_reload FROM <redacted> ORDER BY template_name,short_name;")
	total_jobs = len(paths)

	#MULTIPROCESSING
	p = Pool(multiprocessing.cpu_count()*4, initargs = (counter, ))	
	p.map(process, paths)
	p.close()
	p.join()

	#PROCESSING TIME
	end_time = datetime.datetime.utcnow()
	duration = end_time - start_time
	print("Runtime: "+str(duration))
