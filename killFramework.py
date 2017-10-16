#!/usr/bin/python
import os
import sys
import time
import datetime
import uuid
import json
import glob
import shutil # move file

import logging

import requests
import util



# set some key var
curr_py_path = os.path.realpath(__file__) # current running file - abs path
curr_py_dir, curr_py_filename = os.path.split(curr_py_path)  # current file and folder - abs path

# get proc time - [0] proc date yyyymmdd; [1] proc time hhmmssiii (last 3 millisec)
procDatetimeArr = datetime.datetime.now().strftime('%Y%m%d %H%M%S%f').split(' ')
procDatetimeArr[1] = procDatetimeArr[1][:-3]


# argv[1] - option json string (optional); 
#           default
#            '{
#              "zkStr" : "zk://mesos_master_01:2181,mesos_master_02:2181,mesos_master_03:2181/mesos",
#              "master" : "mesos_master_01", 
#              "masterPort" : 5050,
#              "dispatcherPort" : 7077
#              "mode" : "old",      # info or all or old
#              "keepNumMin" : "60", # keep tasks from last n min
#              "stageFilter" : "",  # "1|2|3a|3b" - only these (only support OR)
#              "extraFilter" : "",  # "abc&def&ghi" - anything string to search (only support AND)
#              "logfile" : "", - empty = no log file
#              "tech" : "all",      # future
#              "vendor" : "all",   # future
#              "oss" : ""           # future
#             }'

if len(sys.argv) < 2:
   util.logMessage("Error: param incorrect.")
   sys.exit(2)

# argv[1] - option json - get first to get all options
optionJSON = ""
if len(sys.argv) > 1:
   optionJSON = sys.argv[1]
if optionJSON == "":
   optionJSON = '{"master":"", "masterPort":5050}'
try:
   optionJSON = json.loads(optionJSON)
except Exception as e: # error parsing json
   optionJSON = '{"master":"", "masterPort":5050}'
   optionJSON = json.loads(optionJSON) 


# default val if not exist
if 'tech' not in optionJSON:
   optionJSON[u'tech'] = "all"
optionJSON[u'tech'] = optionJSON[u'tech'].lower()   
optionJSON[u'techUp'] = optionJSON[u'tech'].upper()   
if 'vendor' not in optionJSON:
   optionJSON[u'vendor'] = "all"
optionJSON[u'vendor'] = optionJSON[u'vendor'].lower()   
optionJSON[u'vendorUp'] = optionJSON[u'vendor'].upper()   
if optionJSON[u'vendorUp'] == 'ERIC':
   optionJSON[u'vendorFULL'] = 'ERICSSON'
elif optionJSON[u'vendorUp'] == 'NOKIA':
   optionJSON[u'vendorFULL'] = 'NOKIA'
else:
   optionJSON[u'vendorFULL'] = 'ALL'
if 'oss' not in optionJSON:
   optionJSON[u'oss'] = ""
if 'zkStr' not in optionJSON:
   #optionJSON[u'zkStr'] = "zk://10.26.156.22:2181,10.26.156.23:2181,10.26.156.24:2181/mesos" # new cluster
   optionJSON[u'zkStr'] = "zk://10.159.121.241:2181,10.159.121.242:2181,10.135.83.106:2181/mesos" # TMO big data cluster
if 'master' not in optionJSON:
   optionJSON[u'master'] = ""
if 'masterPort' not in optionJSON:
   optionJSON[u'masterPort'] = 5050
if 'dispatcherPort' not in optionJSON:
   optionJSON[u'dispatcherPort'] = 7077
if 'logfile' not in optionJSON:
   optionJSON[u'logfile'] = ""
if 'mode' not in optionJSON:
   optionJSON[u'mode'] = "old" # all or old
optionJSON[u'mode'] = optionJSON[u'mode'].lower()
if optionJSON[u'mode'] != "old" and optionJSON[u'mode'] != "all" and optionJSON[u'mode'] != "info":
   optionJSON[u'mode'] = "old"
if 'keepNumMin' not in optionJSON:
   optionJSON[u'keepNumMin'] = "60"
if 'stageFilter' not in optionJSON:
   optionJSON[u'stageFilter'] = "" # 1|2|3a|3b
stgFilterArr = optionJSON[u'stageFilter'].split('|')
if 'extraFilter' not in optionJSON:
   optionJSON[u'extraFilter'] = ""
extraFilterArr = optionJSON[u'extraFilter'].split('&')




# init logger
util.loggerSetup(__name__, optionJSON[u'logfile'], logging.DEBUG)

   

# update master info
# logic: if master provided, ignore zkStr and set master
#        else if zkStr provided, use it to find master
#        else if zkStr empty, use default zkStr (zk://mesos_master_01:2181,mesos_master_02:2181,mesos_master_03:2181/mesos) to find master
#        if still cannot find master, use default (mesos_master_01)
def updateMasterInfo():
	global optionJSON

	if optionJSON[u'master'] != '': # if master defined, not using zookeeper
	   optionJSON[u'zkStr'] = ''
	   util.logMessage("Master default at %s:%d" % (optionJSON[u'master'], optionJSON[u'masterPort']))
	else: # if master not defined, use zookeeper
	   if optionJSON[u'zkStr'] != '':
	      util.logMessage("Try to determine master using zookeeper string: %s" % optionJSON[u'zkStr'])
	      master, masterPort = util.getMesosMaster(optionJSON[u'zkStr'])
	   else:
	      util.logMessage("Try to determine master using default zookeeper string: %s" % 
			"zk://mesos_master_01:2181,mesos_master_02:2181,mesos_master_03:2181/mesos")
	      master, masterPort = util.getMesosMaster()
	   if master == '': # master not found through zookeeper
	      optionJSON[u'master'] = "mesos_master_01"
	      util.logMessage("Cannot get master from zookeeper; master default at %s:%d" % (optionJSON[u'master'], optionJSON[u'masterPort']))
	   else: # master found through zookeeper
	      optionJSON[u'master'] = master
	      optionJSON[u'masterPort'] = masterPort
	      util.logMessage("Master detected at %s:%d" % (optionJSON[u'master'], optionJSON[u'masterPort']))

# get status JSON
def getStatusJSON_mesos():
	js = {}

	#resp = requests.get('http://mesos_master_01:5050/tasks')
	#resp = requests.get('http://mesos_master_01:5050/state')
	#resp = requests.get('http://10.26.126.202:5050/state-summary')
	resp = requests.get("http://%s:%d/master/state-summary" % (optionJSON[u'master'], optionJSON[u'masterPort']))
	if resp.status_code != 200:
		# This means something went wrong.
		#raise ApiError('GET /tasks/ {}'.format(resp.status_code))
		pass
	else:
		js = resp.json()

	return js

# get stats JSON 2
def getStatusJSON_mesos_2(str):
	js = {}

	# tasks, state, state-summary
	resp = requests.get("http://%s:%d/master/%s" % (optionJSON[u'master'], optionJSON[u'masterPort'], str))
	if resp.status_code != 200:
		# This means something went wrong.
		#raise ApiError('GET /tasks/ {}'.format(resp.status_code))
		pass
	else:
		js = resp.json()

	return js


# get cores used
def getCoresUsed_mesos(statusJSON):

	maxcores = 8
	cores = 8 # default to max used already
	if len(statusJSON) == 0:
		# This means something went wrong.
		pass
	else:
		maxcores = 0
		cores = 0
		slaves = statusJSON['slaves']
		for slave in slaves:
			maxcores += int(slave['resources']['cpus'])
			cores += int(slave['used_resources']['cpus'])

	return maxcores, cores

# get current job status
def getCurrJobs_mesos(statusJSON):

	#global prev_jobname

	numJobs = 0
	numWaitingJobs = 0
	t_staging = 0
	t_starting = 0
	t_running = 0
	t_killing = 0
	bFoundLastSubmit = False
	if len(statusJSON) == 0:
		return -1, -1, False
	else:
		jobsArr = statusJSON['frameworks']
		for job in jobsArr:
			if (job['name'].upper().find('MARATHON') == -1 and 
				job['name'].upper().find('CHRONOS') == -1 and
				job['name'].upper().find('SPARK CLUSTER') == -1):
				numJobs += 1
				# further check for waiting task
				if (job['active'] is True and 
					job['TASK_STAGING'] == 0 and
					job['TASK_STARTING'] == 0 and
					job['TASK_RUNNING'] == 0 and
					job['TASK_KILLING'] == 0 and
					job['TASK_FINISHED'] == 0 and
					job['TASK_KILLED'] == 0 and
					job['TASK_FAILED'] == 0 and
					job['TASK_LOST'] == 0 and
					job['TASK_ERROR'] == 0 and
					job['used_resources']['cpus'] == 0):
					numWaitingJobs += 1
			'''
			if job['name'] == prev_jobname:
				bFoundLastSubmit = True
				prev_jobname = "" # reset prev job if found
			'''

		slaves = statusJSON['slaves']
		for worker in slaves:
			t_staging += int(worker["TASK_STAGING"])
			t_starting += int(worker["TASK_STARTING"])
			t_running += int(worker["TASK_RUNNING"])
			t_killing += int(worker["TASK_KILLING"])
		# that should be = numJobs in all slaves so not returning
		numRunningJobs = t_staging + t_starting + t_running + t_killing 

	return numJobs, numWaitingJobs, bFoundLastSubmit

# get current worker status
def haveWorkersResource_mesos(statusJSON):

	bWorkerResource = False
	nNoResource = 0
	if len(statusJSON) == 0:
		return bWorkerResource
	else:
		slaves = statusJSON['slaves']
		numWorkers = len(slaves)
		for worker in slaves:
			if worker["resources"]["cpus"] == worker["used_resources"]["cpus"] or worker["resources"]["mem"] == worker["used_resources"]["mem"]:
				nNoResource += 1
		if nNoResource == numWorkers:
			bWorkerResource = False
		else:
			bWorkerResource = True

	return bWorkerResource

# kill framework
def killFramework(id):
   exec_str_kill = "curl -XPOST http://%s:%d/master/teardown -d 'frameworkId=%s'" % (optionJSON[u'master'], optionJSON[u'masterPort'], id)
   #print exec_str_kill
   os.system(exec_str_kill)

# stop driver in dispatcher
def stopDriver(id):
   exec_str_stop = "/opt/spark/bin/spark-submit --master mesos://%s:%d --kill %s" % (optionJSON[u'master'], optionJSON[u'dispatcherPort'], id)
   #print exec_str_stop
   os.system(exec_str_stop)


   
#######################################################################################
# main proc ###########################################################################
def main():

   '''
   # sameple code
   # get status
   statusJSON = getStatusJSON_mesos()
   cores_max, cores_used = getCoresUsed_mesos(statusJSON)
   print 'max:%s, used:%s' % (cores_max, cores_used)
   print 'have resource: %s' % haveWorkersResource_mesos(statusJSON)
   numJobs, numWaitingJobs, bFoundLastSubmit = getCurrJobs_mesos(statusJSON, '1x2c_client')
   print 'numJobs: %s; numWaitingJobs: %s; bFoundLastSubmit: %s' % (numJobs, numWaitingJobs, bFoundLastSubmit)
   exit(0)
   '''

   # get status
   util.logMessage("Getting job status...")
   statusJSON = getStatusJSON_mesos()
   statusJSON_state = getStatusJSON_mesos_2('state') # tasks, state, state-summary
   util.logMessage("Getting job status completed")

   cores_max, cores_used = getCoresUsed_mesos(statusJSON)
   numJobs, numWaitingJobs, bFoundLastSubmit = getCurrJobs_mesos(statusJSON)
   util.logMessage("\n\tCore usage: %s/%s\n\tNum of jobs: %s\n\tNum of waiting jobs: %s" % (cores_used, cores_max, numJobs, numWaitingJobs))

   if optionJSON[u'mode'].lower() == 'info': # only display info
      return 0
   
   # get curr_epoch_time
   curr_epoch_time = time.time()


   # go to each job to check for criteria to kill
   killedCtr = 0
   stoppedCtr = 0
   if len(statusJSON_state) > 0:
      jobsArr = statusJSON_state['frameworks']
      for job in jobsArr:
         #print job
         # init
         startTime = 'NA'
         reregTime = 'NA'
         endTime = 'NA'
         spanTime = 'NA'
         age = 0 # init

         # stop driver (stg2)
         if job['name'].upper().find('SPARK CLUSTER') != -1:         
            for stg2Driver in job['tasks']: # for each driver in the spark cluster

               # stage filters
               bNeedKill = False
               if stg2Driver['name'].upper().find('DRIVER FOR KPI_PARSER') == -1: # not 15min kpi tasks
                  bNeedKill = False
               else: # 15min kpi related
                  if optionJSON[u'stageFilter'] == "" or optionJSON[u'stageFilter'].find('2') != -1: # no stage filter, or stg2 found in stage filter, need kill
                     bNeedKill = True

               if not bNeedKill: # no need to kill, skip to next job
                  continue
               
               if optionJSON[u'mode'] == "all": # kill all mode
                  util.logMessage("Kill all mode, send stop driver %s: %s" % (stg2Driver['id'], stg2Driver['name']))
                  # stop driver (stg2)
                  stopDriver(stg2Driver['id'])
                  util.logMessage("stop driver submitted.")
                  stoppedCtr += 1
               else:

                  # get start time and calculate age
                  if 'statuses' in stg2Driver:
                     statuses = stg2Driver['statuses']
                     for status in statuses:
                        if 'state' in status and status['state'] == 'TASK_RUNNING':
                           #startTime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(status['timestamp']))
                           #print 'driver start: %s' % startTime
                           age = curr_epoch_time - float(status['timestamp']) # num of sec passed since job started
                           break
                                             
                  if int(age) > int(optionJSON[u'keepNumMin']) * 60: # kill older mode
                     util.logMessage("Job runs longer than %s min (%0.2f), send stop driver %s: %s" % (
                                     optionJSON[u'keepNumMin'], age/60.0, stg2Driver['id'], stg2Driver['name']))
                     # stop driver (stg2)
                     stopDriver(stg2Driver['id'])
                     util.logMessage("stop driver submitted.")
                     stoppedCtr += 1
                  else:
                     #util.logMessage("Job not exceed %s min (%0.2f), no kill job %s: %s" % (
                     #                optionJSON[u'keepNumMin'], age/60.0, stg2Driver['id'], stg2Driver['name']))
                     pass
                  

         # safeguard
         if (job['name'].upper().find('MARATHON') != -1 or 
             job['name'].upper().find('CHRONOS') != -1 or
             job['name'].upper().find('SPARK CLUSTER') != -1):
            continue
         
         if 'registered_time' in job and int(job['registered_time']) != 0:
            startTime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(job['registered_time']))
            age = curr_epoch_time - float(job['registered_time']) # num of sec passed since job started
         if 'reregistered_time' in job and int(job['reregistered_time']) != 0:
            reregTime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(job['reregistered_time']))
         if 'unregistered_time' in job and int(job['unregistered_time']) != 0:
            endTime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(job['unregistered_time']))
         if ('registered_time' in job and int(job['registered_time']) != 0 and 
             'unregistered_time' in job and int(job['unregistered_time']) != 0):
            spanTime = "%0.2f sec" % float(job['unregistered_time']) - float(job['registered_time'])            
         
         #util.logMessage("id: %s - name: %s - start: %s - end: %s - rereg: %s" % (job['id'], job['name'], startTime, endTime, reregTime))
         #util.logMessage("span: %s - age: %0.2f sec" % (spanTime, age))

         # stage filters
         bNeedKill = False
         if job['name'].upper().find('TTSKPI') == -1 or job['name'].upper().find('STG2') != -1: # not 15min kpi tasks or not stg2 (we don't kill stg2 here)
            bNeedKill = False
         else: # 15min kpi related
            if len(stgFilterArr) <= 0: # no stageFilter array, need kill
               bNeedKill = True
            else:
               for stage in stgFilterArr:
                  stargeSearchStr = ("stg"+stage).upper()
                  if stargeSearchStr != 'STG2' and job['name'].upper().find(stargeSearchStr) != -1: # text found (but not stg2, we don't kill stg2 here)
                     bNeedKill = True
                     break

         # extra text filter
         if bNeedKill and len(extraFilterArr) > 0:
            for extraFilter in extraFilterArr:
               if job['name'].upper().find(extraFilter.upper()) == -1: # text not found in current job
                  bNeedKill = False
                    
         if not bNeedKill: # no need to kill, skip to next job
            continue

         if optionJSON[u'mode'] == "all": # kill all mode
            util.logMessage("Kill all mode, send kill job %s: %s" % (job['id'], job['name']))
            # kill framework (stg1,3)
            killFramework(job['id'])
            util.logMessage("kill submitted.")
            killedCtr += 1
         else:
            if int(age) > int(optionJSON[u'keepNumMin']) * 60: # kill older mode
               util.logMessage("Job runs longer than %s min (%0.2f), kill job %s: %s" % (
                               optionJSON[u'keepNumMin'], age/60.0, job['id'], job['name']))
               # kill framework (stg1,3)
               killFramework(job['id'])
               util.logMessage("kill submitted.")
               killedCtr += 1


            else:
               #util.logMessage("Job not exceed %s min (%0.2f), no kill job %s: %s" % (
               #                optionJSON[u'keepNumMin'], age/60.0, job['id'], job['name']))
               pass



   util.logMessage("%d driver (stg2) stop submitted." % stoppedCtr)
   util.logMessage("%d jobs kill submitted." % killedCtr)



if __name__ == "__main__":

   try:
      # Execute Main functionality
      updateMasterInfo() # update master from zkStr
      util.logMessage("framework kill process started with option:\n%s" % json.dumps(optionJSON, sort_keys=True, indent=3)) # pretty print option JSON
      ret = main()
      util.logMessage("framework kill process ended")
      sys.exit(ret)
   except SystemExit as e: # caught sys.exit
      if e.code == 0: # no problem
         pass
      else: # other exception
         raise
   except Exception as e:
      util.logMessage("Error: Main Proc exception occur\n%s" % e)
      util.logMessage("Process terminated.")
      sys.exit(1)
   except:
      util.logMessage("Unexpected error")
      util.logMessage("Process terminated.")
      sys.exit(1)

	
