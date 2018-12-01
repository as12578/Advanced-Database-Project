import LockManager
import DataManager

class Site:
	def __init__(self, site):
		self.site = site
		self.LM = LockManager.LockManager(site)
		self.DM = DataManager.DataManager(site)

	def print(self):
		print('Granted Locks:\n\t%s'%'\n\t'.join(filter(lambda str: not str.endswith(': '), map(lambda key: key + ': ' + ', '.join(map(lambda x: x['transaction'] + ':' + x['type'].name, self.LM.grantedLocks[key])), self.LM.grantedLocks))))
		print('Waiting Locks:\n\t%s'%'\n\t'.join(filter(lambda str: not str.endswith(': '), map(lambda key: key + ': ' + ', '.join(map(lambda x: x['transaction'] + ':' + x['type'].name, self.LM.waitingLocks[key])), self.LM.waitingLocks))))
		print('All Data:\n\t%s'%'\n\t'.join(map(lambda key: key + ': ' + ' '.join(map(lambda dataForKey: ':'.join([dataForKey['transaction'], dataForKey['value'], '%d'%dataForKey['committedTime']]), self.DM.data[key])), self.DM.data)))
