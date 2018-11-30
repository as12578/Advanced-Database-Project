import LockManager
import DataManager

class Site:
	def __init__(self, site):
		self.site = site
		self.LM = LockManager.LockManager(site)
		self.DM = DataManager.DataManager(site)