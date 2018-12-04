import TransactionManager
import SiteManager

class DatabaseManager:
	TM = TransactionManager.TransactionManager
	SM = SiteManager.SiteManager

	def init(time):
		DatabaseManager.SM.init(time)

	def dumpAll():
		for site in DatabaseManager.SM.sites:
			DatabaseManager.SM.sites[site]['site'].DM.dump()

	def dumpKey(key):
		key_index = int(key[1:])
		sites = DatabaseManager.SM.findSitesForKeyIndex(key_index)
		for site in sites:
			site.DM.dumpKey(key)
