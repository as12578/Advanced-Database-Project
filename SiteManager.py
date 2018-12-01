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
		# TODO: Check failure Scenarios
		# TODO: Abort transactions which hold lock to site
		SiteManager.sites[site]['available'] = False
		SiteManager.sites[site]['site'].LM.resetLocks()
		TransactionManager.TransactionManager.notifySiteFailed(site)

	def recover(site, time):
		# TODO: Check recover Scenarios
		# Aborted txns. etc.
		SiteManager.sites[site]['available'] = True
		SiteManager.sites[site]['startTime'] = Timer.CURRENT_TIME
		SiteManager.dumpSite(site)
		# Recover data
		for key in SiteManager.sites[site]['site'].DM.data.keys():
			key_index = int(key[1:])
			sites = SiteManager.findSitesForKeyIndex(key_index)
			for copySite in sites:
				if SiteManager.sites[str(copySite.site)]['available'] and copySite.site != site:
					# Copy data
					SiteManager.sites[site]['site'].DM.data[key] = copySite.DM.data[key][:]# list(filter(lambda data: data['committedTime'] != -1, copySite.DM.data[key]))
					break

		SiteManager.doPendingOperations(site)
		SiteManager.dumpSite(site)

	def querySiteState(site):
		return SiteManager.sites[site]['state']

	def dumpSite(site):
		SiteManager.sites[site]['site'].DM.dump()

	def doPendingOperations(site):
		TM = TransactionManager.TransactionManager
		for pendingOperation in SiteManager.sites[site]['pendingOperations']:
			transaction = pendingOperation['transaction']

			# Delete pending operations at all other sites for transaction
			for otherSite in SiteManager.sites.keys():
				sitePendingOperations = SiteManager.sites[otherSite]['pendingOperations']
				SiteManager.sites[otherSite]['pendingOperations'] = list(filter(lambda pendingOperation: pendingOperation['transaction'] != transaction, SiteManager.sites[otherSite]['pendingOperations']))

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
			print()
		print('======================================================================')
