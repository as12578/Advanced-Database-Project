from enum import Enum
import DatabaseManager
from LockManager import LockType
import Timer

class Operation(Enum):
	READ = 1
	WRITE = 2
	NONE = 0

class TransactionManager:
	transactions = {}

	def beginTransaction(transactionName, startTime, readOnly):
		TransactionManager.transactions[transactionName] = {
				'readOnly': readOnly,
				'pendingOperation': {
					'operation': Operation.NONE,
					'options': {}
				},
				'startTime': startTime,
				'locks': {},
				'failed': False
			}

	def endTransaction(transactionName, endTime):
		SM = DatabaseManager.DatabaseManager.SM

		commitValues = False

		if not TransactionManager.shouldTransactionAbort(transactionName):
			commitValues = True

		if commitValues:
			print('%s commits'%transactionName)
		else:
			print('%s aborts'%transactionName)

		transactionLocks = TransactionManager.transactions[transactionName]['locks']
		for key in transactionLocks.keys():
			commitData = commitValues
			for site in transactionLocks[key].keys():
				commitData = commitData and 'lockType' in transactionLocks[key][site] and transactionLocks[key][site]['lockType'] == LockType.EXCLUSIVE

			key_index = int(key[1:])
			sites = SM.findSitesForKeyIndex(key_index)
			for site in sites:
				if commitData:
					site.DM.commitTransactionKey(transactionName, key, endTime)
				else:
					site.DM.abortTransactionKey(transactionName, key, endTime)

				site.LM.releaseLock(transactionName, key)

		del TransactionManager.transactions[transactionName]

	def shouldTransactionAbort(transaction):
		if TransactionManager.transactions[transaction]['failed']:
			return True

		for key in TransactionManager.transactions[transaction]['locks']:
			for site in TransactionManager.transactions[transaction]['locks'][key]:
				# site where lock was obtained and value read/written. If site failed after first access, abort
				SM = DatabaseManager.DatabaseManager.SM
				if 'firstGrant' in TransactionManager.transactions[transaction]['locks'][key][site] and (not SM.sites[site]['available'] or TransactionManager.transactions[transaction]['locks'][key][site]['firstGrant'] < SM.sites[site]['startTime']):
					return True

		return False

	def _pendingOperationToString(pendingOperation):
		operationOptions = pendingOperation['options']

		if pendingOperation['operation'] == Operation.NONE:
			return ''

		if pendingOperation['operation'] == Operation.READ:
			return 'Read %s'%operationOptions['key']

		return '\nWrite %s: %s\nPending Sites: %s \nWritten Sites: %s'%(operationOptions['key'], operationOptions['value'], operationOptions['pendingSites'], operationOptions['writtenSites'])

	def print():
		print('\n=========================Current Transactions=========================')

		for transaction in TransactionManager.transactions:
			print('Transaction:', transaction)
			print('Read Only:', TransactionManager.transactions[transaction]['readOnly'])
			print('Start Time:', TransactionManager.transactions[transaction]['startTime'])
			print('Aborted:', TransactionManager.transactions[transaction]['failed'])
			lockObj = TransactionManager.transactions[transaction]['locks']
			print('Locks:\n\t%s'%'\n\t'.join(map(lambda key: key + ': ' + '   '.join(map(lambda site: ':'.join([site, lockObj[key][site]['lockType'].name, '%d'%lockObj[key][site]['firstGrant']]), filter(lambda site: 'lockType' in lockObj[key][site], lockObj[key]))), lockObj)))
			print('Pending Operation:', TransactionManager._pendingOperationToString(TransactionManager.transactions[transaction]['pendingOperation']))
			print()

		print('======================================================================')

	def detectDeadlock():
		# TODO: Deadlock cycle detection
		pass

	def readValue(transactionName, key):
		# Read from one site
		if TransactionManager.transactions[transactionName]['failed']:
			return

		SM = DatabaseManager.DatabaseManager.SM
		key_index = int(key[1:])
		sites = SM.findSitesForKeyIndex(key_index)

		TransactionManager.transactions[transactionName]['pendingOperation'] = {
			'operation': Operation.READ,
			'options': {
				'key': key
			}
		}

		if TransactionManager.transactions[transactionName]['readOnly']:
			failedSites = []

			for site in sites:
				if SM.sites[site.site]['available'] == False:
					failedSites.append(site)
				else:
					TransactionManager.doPendingOperation(transactionName, site.site)
					break

			# If all sites have failed
			if len(failedSites) == len(sites):
				for site in failedSites:
					SM.sites[site.site]['pendingOperations'].append({
						'transaction': transactionName,
						'operation': Operation.READ,
						'options': {
							'key': key
						}
					})

			return

		# Init TransactionManager.transactions[transactionName]['locks'][key]
		if key not in TransactionManager.transactions[transactionName]['locks']:
			TransactionManager.transactions[transactionName]['locks'][key] = {}

		failedSites = []
		for site in sites:
			if SM.sites[site.site]['available'] == False:
				failedSites.append(site)
			elif key in TransactionManager.transactions[transactionName]['locks'] and site.site in TransactionManager.transactions[transactionName]['locks'][key] and 'lockType' in TransactionManager.transactions[transactionName]['locks'][key][site.site]:
				# Already have a lock. Continue to read from site
				TransactionManager.doPendingOperation(transactionName, site.site)
				# TODO: Should we cancel lock requests on other sites?
				break
			else:
				TransactionManager.transactions[transactionName]['locks'][key][site.site] = {}
				site.LM.requestLock(transactionName, key, LockType.SHARED)
				# Check if operation done. Check if lock granted. since we are doing everything sequentially
				if key in TransactionManager.transactions[transactionName]['locks'] and site.site in TransactionManager.transactions[transactionName]['locks'][key] and 'lockType' in TransactionManager.transactions[transactionName]['locks'][key][site.site]:
					# TODO: Should we cancel lock requests on other sites?
					pass
				break

		# If all sites have failed
		if len(failedSites) == len(sites):
			for site in failedSites:
				if key not in TransactionManager.transactions[transactionName]['locks']:
					TransactionManager.transactions[transactionName]['locks'][key] = {}
				TransactionManager.transactions[transactionName]['locks'][key][site.site] = {}
				SM.sites[site.site]['pendingOperations'].append(TransactionManager.getSitePendingOperationFromTransaction(transactionName))

	def writeValue(transactionName, key, value):
		# Write to all available copies

		if TransactionManager.transactions[transactionName]['failed']:
			return

		SM = DatabaseManager.DatabaseManager.SM
		key_index = int(key[1:])
		sites = SM.findSitesForKeyIndex(key_index)

		TransactionManager.transactions[transactionName]['pendingOperation'] = {
			'operation': Operation.WRITE,
			'options': {
				'key': key,
				'value': value,
				'pendingSites': sites,
				'writtenSites': []
			}
		}

		if key not in TransactionManager.transactions[transactionName]['locks']:
			TransactionManager.transactions[transactionName]['locks'][key] = {}

		for site in sites:
			failedSites = []
			if SM.sites[site.site]['available'] == False:
				failedSites.append(site)
				TransactionManager.transactions[transactionName]['pendingOperation']['options']['pendingSites'] = list(filter(lambda pendingSite: pendingSite.site != site.site, TransactionManager.transactions[transactionName]['pendingOperation']['options']['pendingSites']))
			elif key in TransactionManager.transactions[transactionName]['locks'] and site.site in TransactionManager.transactions[transactionName]['locks'][key] and 'lockType' in TransactionManager.transactions[transactionName]['locks'][key][site.site] and TransactionManager.transactions[transactionName]['locks'][key][site.site]['lockType'] == LockType.EXCLUSIVE:
				# Site is up and transaction has write lock for the key on the site, the do operation
				TransactionManager.doPendingOperation(transactionName, site.site)
			else:
				# Site is up but transaction does not have a write lock for the key on the site, then request write lock
				TransactionManager.transactions[transactionName]['locks'][key][site.site] = {}
				site.LM.requestLock(transactionName, key, LockType.EXCLUSIVE)

		if len(failedSites) == len(sites):
			# Not updating pendingSites since a write to the first available site will suffice
			for site in failedSites:
				if key not in TransactionManager.transactions[transactionName]['locks']:
					TransactionManager.transactions[transactionName]['locks'][key] = {}
				TransactionManager.transactions[transactionName]['locks'][key][site.site] = {}
				SM.sites[site.site]['pendingOperations'].append(TransactionManager.getSitePendingOperationFromTransaction(transactionName))

	def grantLock(transactionName, site, lockType):
		pendingOperation = TransactionManager.transactions[transactionName]['pendingOperation']
		key = pendingOperation['options']['key']
		if 'firstGrant' not in TransactionManager.transactions[transactionName]['locks'][key][site]:
			TransactionManager.transactions[transactionName]['locks'][key][site] = {
				'firstGrant': Timer.CURRENT_TIME,
				'lockType': lockType
			}
		TransactionManager.transactions[transactionName]['locks'][key][site]['lockType'] = lockType
		TransactionManager.doPendingOperation(transactionName, site)

	def doPendingOperation(transactionName, site):
		SM = DatabaseManager.DatabaseManager.SM
		pendingOperation = TransactionManager.transactions[transactionName]['pendingOperation']

		key = pendingOperation['options']['key']

		clearOp = True
		if pendingOperation['operation'] == Operation.READ and TransactionManager.transactions[transactionName]['readOnly']:
			value = SM.sites[site]['site'].DM.readVersionAtTime(transactionName, key, TransactionManager.transactions[transactionName]['startTime'])
			print('%s - %s: %s'%(transactionName, key, value))
		elif pendingOperation['operation'] == Operation.READ:
			valueObj = SM.sites[site]['site'].DM.getValue(transactionName, key)
			print('%s - %s: %s'%(transactionName, key, valueObj['value']))
		elif pendingOperation['operation'] == Operation.WRITE:
			SM.sites[site]['site'].DM.setValue(transactionName, key, pendingOperation['options']['value'])
			TransactionManager.transactions[transactionName]['pendingOperation']['options']['writtenSites'].append(site)
			TransactionManager.transactions[transactionName]['pendingOperation']['options']['pendingSites'] = list(filter(lambda pendingSite: pendingSite.site != site, TransactionManager.transactions[transactionName]['pendingOperation']['options']['pendingSites']))
			clearOp = len(TransactionManager.transactions[transactionName]['pendingOperation']['options']['pendingSites']) == 0

		if clearOp:
			TransactionManager.transactions[transactionName]['pendingOperation'] = {
				'operation': Operation.NONE,
				'options': {}
			}

	def notifySiteFailed(site):
		SM = DatabaseManager.DatabaseManager.SM
		for transaction in TransactionManager.transactions:
			transactionFailed = False
			transactionDetails = TransactionManager.transactions[transaction]
			for key in transactionDetails['locks']:
				for lockSite in transactionDetails['locks'][key]:
					if site == lockSite and transactionDetails['locks'][key][lockSite] != {}:
						TransactionManager.transactions[transaction]['failed'] = True
						transactionFailed = True
						break
					elif site == lockSite:
						# Transaction was waiting for a lock on site
						if key not in TransactionManager.transactions[transaction]['locks']:
							TransactionManager.transactions[transaction]['locks'][key][site.site] = {}

						SM.sites[site]['pendingOperations'].append(TransactionManager.getSitePendingOperationFromTransaction(transaction))

				if transactionFailed:
					break

			if transactionFailed:
				TransactionManager.transactions[transaction]['pendingOperation'] = {
					'operation': Operation.NONE,
					'options': {}
				}

	def getSitePendingOperationFromTransaction(transaction):
		transactionPendingOperation = TransactionManager.transactions[transaction]['pendingOperation']
		pendingOperation = {
			'transaction': transaction,
			'operation': transactionPendingOperation['operation']
		}

		if pendingOperation['operation'] == Operation.NONE:
			return {}
		elif pendingOperation['operation'] == Operation.READ:
			pendingOperation['options'] = {
				'key': transactionPendingOperation['options']['key']
			}
		else:
			pendingOperation['options'] = {
				'key': transactionPendingOperation['options']['key'],
				'value': transactionPendingOperation['options']['value']
			}

		return pendingOperation