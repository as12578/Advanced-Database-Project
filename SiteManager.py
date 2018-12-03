import Site
import TransactionManager
import Timer
import LockManager

NUM_SITES = 10
NUM_KEYS = 20

class SiteManager:
	sites = {}
	def init(startTime):
		for i in range(1, NUM_SITES + 1):
			SiteManager.sites[str(i)] = {
				'site': Site.Site(str(i)),
				'available': True,
				'pendingOperations': [],
				'startTime': startTime
			}

		for key_index in range(1, NUM_KEYS + 1):
			key = 'x' + str(key_index)

			sites = SiteManager.findSitesForKeyIndex(key_index)

			for site in sites:
				site.DM.initValue(key, str(10 * key_index))
				site.LM.initLockForKey(key)

	def findSitesForKeyIndex(key_index):
		site_indexes = []
		if key_index%2 == 0:
			site_indexes = range(1, NUM_SITES + 1)
		else:
			site_indexes = [1 + key_index % 10]

		return list(map(lambda site_index: SiteManager.sites[str(site_index)]['site'], site_indexes))

	def fail(site):
		SiteManager.sites[site]['available'] = False
		SiteManager.sites[site]['site'].LM.resetLocks()
		TransactionManager.TransactionManager.notifySiteFailed(site)

	def recover(site, time):
		SiteManager.sites[site]['available'] = True
		SiteManager.sites[site]['startTime'] = Timer.CURRENT_TIME
		SiteManager.doRecoveryAllowedPendingOperations(site)

	def querySiteState(site):
		return SiteManager.sites[site]['state']

	def dumpSite(site):
		SiteManager.sites[site]['site'].DM.dump()

	def doRecoveryAllowedPendingOperations(site):
		TM = TransactionManager.TransactionManager
		for pendingOperation in SiteManager.sites[site]['pendingOperations']:
			transaction = pendingOperation['transaction']

			key = pendingOperation['options']['key']
			key_index = int(key[1:])

			DM = SiteManager.sites[site]['site'].DM
			if key_index % 2 == 0 and DM.getLastCommitTime(key) < SiteManager.sites[site]['startTime']:
				continue

			# Delete pending operations at all other sites for transaction. Because if a transaction is waiting on multiple sites, it is waiting for the first site to succeed.
			for otherSite in SiteManager.sites:
				sitePendingOperations = SiteManager.sites[otherSite]['pendingOperations']
				SiteManager.sites[otherSite]['pendingOperations'] = list(filter(lambda pendingOperation: pendingOperation['transaction'] != transaction, SiteManager.sites[otherSite]['pendingOperations']))

			# A read only transaction needs no locks
			if TM.transactions[transaction]['readOnly']:
				TM.doPendingOperation(transaction, site)
				return

			lockType = LockManager.LockType.SHARED
			if pendingOperation['operation'] == TransactionManager.Operation.WRITE:
				lockType = LockManager.LockType.EXCLUSIVE

			SiteManager.sites[site]['site'].LM.requestLock(transaction, pendingOperation['options']['key'], lockType)

	def doPendingOperationsForKey(site, keyToRun):
		TM = TransactionManager.TransactionManager
		for pendingOperation in SiteManager.sites[site]['pendingOperations']:
			transaction = pendingOperation['transaction']

			key = pendingOperation['options']['key']
			key_index = int(key[1:])

			if keyToRun != key:
				continue

			DM = SiteManager.sites[site]['site'].DM
			if key_index % 2 == 0 and DM.getLastCommitTime(key) < SiteManager.sites[site]['startTime']:
				continue

			# Delete pending operations at all other sites for transaction. Because if a transaction is waiting on multiple sites, it is waiting for the first site to succeed.
			for otherSite in SiteManager.sites:
				sitePendingOperations = SiteManager.sites[otherSite]['pendingOperations']
				SiteManager.sites[otherSite]['pendingOperations'] = list(filter(lambda pendingOperation: pendingOperation['transaction'] != transaction, SiteManager.sites[otherSite]['pendingOperations']))

			# A read only transaction needs no locks
			if TM.transactions[transaction]['readOnly']:
				TM.doPendingOperation(transaction, site)
				return

			lockType = LockManager.LockType.SHARED
			if pendingOperation['operation'] == TransactionManager.Operation.WRITE:
				lockType = LockManager.LockType.EXCLUSIVE

			SiteManager.sites[site]['site'].LM.requestLock(transaction, pendingOperation['options']['key'], lockType)

	def print():
		print('\n============================All Site State============================')

		for site in SiteManager.sites:
			print('Site:', site)
			print('Start Time:', SiteManager.sites[site]['startTime'])
			print('Available:', SiteManager.sites[site]['available'])
			SiteManager.sites[site]['site'].print()
			print('Pending Operations:', '\n'.join(list(map(SiteManager._pendingOperationToString, SiteManager.sites[site]['pendingOperations']))))
			print()
		print('======================================================================')

	def _pendingOperationToString(pendingOperation):
		operationOptions = pendingOperation['options']

		if pendingOperation['operation'] == TransactionManager.Operation.NONE:
			return ''

		if pendingOperation['operation'] == TransactionManager.Operation.READ:
			return '%s: Read %s'%(pendingOperation['transaction'], operationOptions['key'])

		return '\n%s: Write %s: %s'%(pendingOperation['transaction'], operationOptions['key'], operationOptions['value'])

	def _recoverSiteData(site):
		# Recover data
		for key in SiteManager.sites[site]['site'].DM.data:
			key_index = int(key[1:])
			sites = SiteManager.findSitesForKeyIndex(key_index)
			for copySite in sites:
				# TODO: Copy if condition from below function
				if SiteManager.sites[str(copySite.site)]['available'] and copySite.site != site:
					# Copy data
					SiteManager.sites[site]['site'].DM.data[key] = copySite.DM.data[key][:]
					break

	def recoverKeyData(site, key):
		# Recover data
		key_index = int(key[1:])
		sites = SiteManager.findSitesForKeyIndex(key_index)
		for copySite in sites:
			DM = SiteManager.sites[str(copySite.site)]['site'].DM
			if SiteManager.sites[str(copySite.site)]['available'] and copySite.site != site and DM.getLastCommitTime(key) >= SiteManager.sites[str(copySite.site)]['startTime']:
				# Copy data
				SiteManager.sites[site]['site'].DM.data[key] = copySite.DM.data[key][:]
				break
